from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def newest_csv(directory: Path) -> Path:
    if not directory.exists():
        raise FileNotFoundError(f"Directory not found: {directory}")
    csv_files = [p for p in directory.glob("*.csv") if p.is_file()]
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in: {directory}")
    return max(csv_files, key=lambda p: p.stat().st_mtime)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run build_priority_list.py using the newest needs/source CSV files."
    )
    parser.add_argument("--needs-dir", default="inputs/needs", help="Folder containing needs CSV exports")
    parser.add_argument("--source-dir", default="inputs/source", help="Folder containing source CSV exports")
    parser.add_argument("--output", default="outputs/priority_list.csv", help="Priority output CSV")
    parser.add_argument("--production-output", default="outputs/production_plan.csv", help="Production plan CSV")
    parser.add_argument("--todo-output", default="outputs/team_todo.csv", help="Compiled team to-do CSV")
    parser.add_argument("--station-output-dir", default="outputs/stations", help="Station outputs directory")
    parser.add_argument("--top-n", type=int, default=25, help="Top N production rows")
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python executable to run build_priority_list.py (default: current interpreter)",
    )
    args = parser.parse_args()

    workspace = Path(__file__).resolve().parent.parent
    needs_dir = workspace / args.needs_dir
    source_dir = workspace / args.source_dir

    needs_csv = newest_csv(needs_dir)
    source_csv = newest_csv(source_dir)

    cmd = [
        args.python,
        str(workspace / "src" / "build_priority_list.py"),
        "--inventory",
        str(needs_csv),
        "--source-inventory",
        str(source_csv),
        "--output",
        str(workspace / args.output),
        "--production-output",
        str(workspace / args.production_output),
        "--todo-output",
        str(workspace / args.todo_output),
        "--top-n",
        str(args.top_n),
        "--station-output-dir",
        str(workspace / args.station_output_dir),
    ]

    print(f"Using needs file:  {needs_csv}")
    print(f"Using source file: {source_csv}")
    print("Running pipeline...")
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
