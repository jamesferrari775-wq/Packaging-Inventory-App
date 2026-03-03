from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


@dataclass
class InputPair:
    inventory: Path
    sales: Path


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit(message: str, log_file: Optional[Path] = None) -> None:
    timestamped = f"[{utc_now_iso()}] {message}"
    print(timestamped)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with log_file.open("a", encoding="utf-8") as handle:
            handle.write(timestamped + "\n")


def load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def newest_matching_csv(directory: Path, pattern: re.Pattern[str]) -> Optional[Path]:
    if not directory.exists():
        return None
    candidates = [
        p for p in directory.glob("*.csv") if p.is_file() and pattern.search(p.name.lower())
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime_ns)


def find_input_pair(directory: Path, inventory_pattern: str, sales_pattern: str) -> Optional[InputPair]:
    inv_re = re.compile(inventory_pattern, re.IGNORECASE)
    sales_re = re.compile(sales_pattern, re.IGNORECASE)

    inventory = newest_matching_csv(directory, inv_re)
    sales = newest_matching_csv(directory, sales_re)
    if not inventory or not sales:
        return None
    return InputPair(inventory=inventory, sales=sales)


def find_strict_latest_pair(directory: Path) -> Optional[InputPair]:
    if not directory.exists():
        return None

    by_name = {p.name.lower(): p for p in directory.glob("*.csv") if p.is_file()}
    inventory = by_name.get("latest_inventory.csv")
    sales = by_name.get("latest_sales.csv")
    if not inventory or not sales:
        return None
    return InputPair(inventory=inventory, sales=sales)


def fingerprint(pair: InputPair) -> dict:
    return {
        "inventory_path": str(pair.inventory.resolve()),
        "inventory_mtime_ns": pair.inventory.stat().st_mtime_ns,
        "sales_path": str(pair.sales.resolve()),
        "sales_mtime_ns": pair.sales.stat().st_mtime_ns,
    }


def run_pipeline(
    workspace: Path,
    pair: InputPair,
    python_cmd: str,
    top_n: int,
    log_file: Optional[Path],
) -> tuple[int, str]:
    cmd = [
        python_cmd,
        str(workspace / "src" / "build_priority_list.py"),
        "--inventory",
        str(pair.inventory),
        "--sales",
        str(pair.sales),
        "--output",
        str(workspace / "outputs" / "priority_list.csv"),
        "--production-output",
        str(workspace / "outputs" / "production_plan.csv"),
        "--todo-output",
        str(workspace / "outputs" / "team_todo.csv"),
        "--station-output-dir",
        str(workspace / "outputs" / "stations"),
        "--top-n",
        str(top_n),
    ]

    emit("=== Running priority pipeline ===", log_file)
    emit(f"Inventory: {pair.inventory}", log_file)
    emit(f"Sales:     {pair.sales}", log_file)
    emit("Command: " + " ".join(cmd), log_file)

    result = subprocess.run(cmd, cwd=workspace, capture_output=True, text=True)
    combined_output = "\n".join(part for part in [result.stdout, result.stderr] if part).strip()
    if combined_output:
        emit("Pipeline output:\n" + combined_output, log_file)
    return result.returncode, combined_output


def save_status(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def process_once(
    workspace: Path,
    drop_dir: Path,
    inventory_pattern: str,
    sales_pattern: str,
    strict_latest_names: bool,
    state_path: Path,
    status_path: Path,
    python_cmd: str,
    top_n: int,
    log_file: Optional[Path],
) -> bool:
    pair = find_strict_latest_pair(drop_dir) if strict_latest_names else find_input_pair(
        drop_dir,
        inventory_pattern,
        sales_pattern,
    )

    status_payload = {
        "last_checked_utc": utc_now_iso(),
        "drop_dir": str(drop_dir),
        "strict_latest_names": strict_latest_names,
        "inventory_pattern": inventory_pattern,
        "sales_pattern": sales_pattern,
    }

    if not pair:
        emit(
            "Waiting for files... Need both inventory and sales CSVs in "
            f"{drop_dir}. Mode={'strict latest names' if strict_latest_names else 'regex match'}",
            log_file,
        )
        status_payload.update(
            {
                "last_result": "waiting",
                "last_error": "Required input pair not found",
            }
        )
        save_status(status_path, status_payload)
        return False

    current = fingerprint(pair)
    previous = load_state(state_path)
    if previous == current:
        emit("No new inventory/sales pair detected.", log_file)
        status_payload.update(
            {
                "last_result": "unchanged",
                "inventory": str(pair.inventory),
                "sales": str(pair.sales),
            }
        )
        save_status(status_path, status_payload)
        return False

    code, output = run_pipeline(
        workspace,
        pair,
        python_cmd=python_cmd,
        top_n=top_n,
        log_file=log_file,
    )
    if code == 0:
        save_state(state_path, current)
        emit("Pipeline finished successfully. State updated.", log_file)
        status_payload.update(
            {
                "last_result": "success",
                "last_run_utc": utc_now_iso(),
                "inventory": str(pair.inventory),
                "sales": str(pair.sales),
                "exit_code": code,
            }
        )
        save_status(status_path, status_payload)
        return True

    emit(f"Pipeline failed with exit code {code}. State not updated.", log_file)
    status_payload.update(
        {
            "last_result": "failed",
            "last_run_utc": utc_now_iso(),
            "inventory": str(pair.inventory),
            "sales": str(pair.sales),
            "exit_code": code,
            "last_error": output[-2000:] if output else "Pipeline returned non-zero exit code",
        }
    )
    save_status(status_path, status_payload)
    return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Watch a folder for new inventory+sales CSV pairs and auto-run pipeline."
    )
    parser.add_argument("--drop-dir", default="inputs/auto_drop", help="Folder to watch for incoming CSVs")
    parser.add_argument(
        "--inventory-pattern",
        default=r"(inventory|packages|valuation|needs)",
        help="Regex used to identify inventory CSV filenames",
    )
    parser.add_argument(
        "--sales-pattern",
        default=r"(sales|sku|sold)",
        help="Regex used to identify sales CSV filenames",
    )
    parser.add_argument("--state-file", default="outputs/auto_watch_state.json", help="State file path")
    parser.add_argument("--status-file", default="outputs/auto_watch_status.json", help="Status output JSON path")
    parser.add_argument("--log-file", default="outputs/auto_watch.log", help="Watcher log output file")
    parser.add_argument(
        "--strict-latest-names",
        action="store_true",
        help="Require exact filenames latest_inventory.csv and latest_sales.csv",
    )
    parser.add_argument("--top-n", type=int, default=25, help="Top N production rows")
    parser.add_argument("--python", default=sys.executable, help="Python executable for pipeline command")
    parser.add_argument("--watch", action="store_true", help="Keep watching and run on each new pair")
    parser.add_argument("--interval", type=int, default=20, help="Polling interval in seconds when --watch is used")
    args = parser.parse_args()

    workspace = Path(__file__).resolve().parent.parent
    drop_dir_input = Path(args.drop_dir)
    drop_dir = drop_dir_input if drop_dir_input.is_absolute() else (workspace / drop_dir_input)
    state_path = workspace / args.state_file
    status_path = workspace / args.status_file
    log_file = workspace / args.log_file if args.log_file else None
    drop_dir.mkdir(parents=True, exist_ok=True)

    emit(f"Watching folder: {drop_dir}", log_file)
    if args.strict_latest_names:
        emit("Strict mode: expecting latest_inventory.csv and latest_sales.csv", log_file)
    else:
        emit(
            f"Regex mode: inventory='{args.inventory_pattern}' sales='{args.sales_pattern}'",
            log_file,
        )
    emit(f"Status file: {status_path}", log_file)
    if log_file:
        emit(f"Log file: {log_file}", log_file)

    if not args.watch:
        process_once(
            workspace=workspace,
            drop_dir=drop_dir,
            inventory_pattern=args.inventory_pattern,
            sales_pattern=args.sales_pattern,
            strict_latest_names=args.strict_latest_names,
            state_path=state_path,
            status_path=status_path,
            python_cmd=args.python,
            top_n=args.top_n,
            log_file=log_file,
        )
        return

    emit(f"Watch mode enabled. Polling every {args.interval}s.", log_file)
    while True:
        process_once(
            workspace=workspace,
            drop_dir=drop_dir,
            inventory_pattern=args.inventory_pattern,
            sales_pattern=args.sales_pattern,
            strict_latest_names=args.strict_latest_names,
            state_path=state_path,
            status_path=status_path,
            python_cmd=args.python,
            top_n=args.top_n,
            log_file=log_file,
        )
        time.sleep(max(5, args.interval))


if __name__ == "__main__":
    main()
