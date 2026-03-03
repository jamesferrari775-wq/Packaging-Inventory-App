from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from flask import Flask, abort, jsonify, render_template, request, send_file


WORKSPACE = Path(__file__).resolve().parent.parent
UPLOADS_DIR = WORKSPACE / "inputs" / "web_uploads"
OUTPUTS_DIR = WORKSPACE / "outputs"


def ensure_dirs() -> None:
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)


app = Flask(__name__)


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def is_view_only_mode() -> bool:
    return env_flag("VIEW_ONLY", default=False)


def ingest_token() -> str:
    return os.getenv("INGEST_TOKEN", "").strip()


def save_upload(file_storage, prefix: str) -> Path | None:
    if not file_storage or not file_storage.filename:
        return None
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = Path(file_storage.filename).name
    destination = UPLOADS_DIR / f"{timestamp}_{prefix}_{safe_name}"
    file_storage.save(destination)
    return destination


def run_priority_pipeline(
    inventory_path: Path,
    sales_path: Path | None,
) -> tuple[bool, str]:
    cmd = [
        sys.executable,
        str(WORKSPACE / "src" / "build_priority_list.py"),
        "--inventory",
        str(inventory_path),
        "--output",
        str(OUTPUTS_DIR / "priority_list.csv"),
        "--production-output",
        str(OUTPUTS_DIR / "production_plan.csv"),
        "--todo-output",
        str(OUTPUTS_DIR / "team_todo.csv"),
        "--station-output-dir",
        str(OUTPUTS_DIR / "stations"),
        "--top-n",
        "25",
    ]

    if sales_path:
        cmd.extend(["--sales", str(sales_path)])

    try:
        result = subprocess.run(
            cmd,
            cwd=WORKSPACE,
            capture_output=True,
            text=True,
            check=True,
        )
        output_text = "\n".join(part for part in [result.stdout.strip(), result.stderr.strip()] if part)
        return True, output_text or "Pipeline completed successfully."
    except subprocess.CalledProcessError as exc:
        output_text = "\n".join(part for part in [exc.stdout.strip(), exc.stderr.strip()] if part)
        return False, output_text or "Pipeline failed with no output."


def list_downloads() -> list[dict[str, str]]:
    candidates = [
        OUTPUTS_DIR / "priority_list.csv",
        OUTPUTS_DIR / "production_plan.csv",
        OUTPUTS_DIR / "team_todo.csv",
        OUTPUTS_DIR / "stations" / "cart_queue.csv",
        OUTPUTS_DIR / "stations" / "unit_queue.csv",
        OUTPUTS_DIR / "stations" / "preroll_queue.csv",
        OUTPUTS_DIR / "stations" / "cart_todo.csv",
        OUTPUTS_DIR / "stations" / "unit_todo.csv",
        OUTPUTS_DIR / "stations" / "preroll_todo.csv",
    ]

    files: list[dict[str, str]] = []
    for path in candidates:
        if path.exists() and path.is_file():
            rel = path.relative_to(WORKSPACE).as_posix()
            files.append({"name": path.name, "rel_path": rel})
    return files


def list_tile_views() -> list[dict[str, str]]:
    candidates = [
        ("All Stations", OUTPUTS_DIR / "stations" / "station_todo_tiles.html"),
        ("Cart Station", OUTPUTS_DIR / "stations" / "cart_station_tiles.html"),
        ("Unit Station", OUTPUTS_DIR / "stations" / "unit_station_tiles.html"),
        ("Preroll Station", OUTPUTS_DIR / "stations" / "preroll_station_tiles.html"),
    ]

    views: list[dict[str, str]] = []
    for label, path in candidates:
        if path.exists() and path.is_file():
            rel = path.relative_to(WORKSPACE).as_posix()
            views.append({"name": label, "rel_path": rel})
    return views


@app.route("/", methods=["GET", "POST"])
def index():
    ensure_dirs()
    message = ""
    success = None
    log_output = ""
    view_only = is_view_only_mode()

    if request.method == "POST" and not view_only:
        inventory_file = request.files.get("inventory")
        sales_file = request.files.get("sales")

        if not inventory_file or not inventory_file.filename:
            message = "Inventory CSV is required."
            success = False
        else:
            inventory_path = save_upload(inventory_file, "inventory")
            sales_path = save_upload(sales_file, "sales")

            ok, logs = run_priority_pipeline(inventory_path, sales_path)
            success = ok
            message = "Priority list generated." if ok else "Pipeline failed."
            log_output = logs

    return render_template(
        "index.html",
        message=message,
        success=success,
        log_output=log_output,
        downloads=list_downloads(),
        tile_views=list_tile_views(),
        view_only=view_only,
    )


@app.get("/download/<path:rel_path>")
def download(rel_path: str):
    workspace_resolved = WORKSPACE.resolve()
    target = (workspace_resolved / rel_path).resolve()
    try:
        target.relative_to(workspace_resolved)
    except ValueError:
        abort(400)
    if not target.exists() or not target.is_file():
        abort(404)
    return send_file(target, as_attachment=True)


@app.get("/view/<path:rel_path>")
def view_file(rel_path: str):
    workspace_resolved = WORKSPACE.resolve()
    target = (workspace_resolved / rel_path).resolve()
    try:
        target.relative_to(workspace_resolved)
    except ValueError:
        abort(400)
    if target.suffix.lower() != ".html":
        abort(400)
    if not target.exists() or not target.is_file():
        abort(404)
    return send_file(target, as_attachment=False, mimetype="text/html")


@app.get("/stations")
def stations_all():
    return view_file("outputs/stations/station_todo_tiles.html")


@app.get("/stations/cart")
def stations_cart():
    return view_file("outputs/stations/cart_station_tiles.html")


@app.get("/stations/unit")
def stations_unit():
    return view_file("outputs/stations/unit_station_tiles.html")


@app.get("/stations/preroll")
def stations_preroll():
    return view_file("outputs/stations/preroll_station_tiles.html")


@app.get("/favicon.ico")
def favicon():
    return ("", 204)


@app.post("/api/ingest")
def ingest():
    token = ingest_token()
    if not token:
        return jsonify({"ok": False, "error": "INGEST_TOKEN is not configured on server"}), 500

    provided = request.headers.get("X-Ingest-Token", "").strip()
    if provided != token:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    inventory_file = request.files.get("inventory")
    sales_file = request.files.get("sales")

    if not inventory_file or not inventory_file.filename:
        return jsonify({"ok": False, "error": "Missing required file: inventory"}), 400

    ensure_dirs()
    inventory_path = save_upload(inventory_file, "inventory")
    sales_path = save_upload(sales_file, "sales") if sales_file and sales_file.filename else None

    ok, logs = run_priority_pipeline(inventory_path, sales_path)
    status_code = 200 if ok else 500
    return (
        jsonify(
            {
                "ok": ok,
                "message": "Priority list generated" if ok else "Pipeline failed",
                "outputs": {
                    "priority": "outputs/priority_list.csv",
                    "production": "outputs/production_plan.csv",
                    "todo": "outputs/team_todo.csv",
                    "stations": "outputs/stations",
                },
                "logs": logs,
            }
        ),
        status_code,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Local upload UI for priority pipeline")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=5050, type=int)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    ensure_dirs()
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
