# Packaging + Inventory Priority

Build a repeatable prioritized reorder list from two Distru exports:

1. Inventory valuation export (SKU-level stock)
2. Optional: sales export with SKU + quantity sold

You can also use the Distru **Packages** export as the inventory source. The script aggregates package rows by `Distru Product` and uses `Lab Testing State` directly.

Output is a CSV that can be opened directly in Google Sheets.

## Files

- `src/build_priority_list.py` - Main script
- `src/run_latest_pipeline.py` - Runs pipeline using newest files in `inputs/needs` + `inputs/source`
- `config/priority_rules.json` - Tunable rules and column aliases
- `config/google_sheets_setup.md` - Pure Google Sheets formula setup
- `config/automation_setup.md` - End-to-end Gmail/Drive/Sheets automation guide
- `config/apps_script_gmail_to_drive.gs` - Google Apps Script: Gmail attachment ingest to Drive
- `config/apps_script_drive_to_sheets.gs` - Google Apps Script: CSVs from Drive into Sheet tabs

## Run

From the workspace root:

```bash
python src/build_priority_list.py --inventory "path/to/distru-inventory-valuation-report.csv" --sales "path/to/distru-sales-sku-report.csv" --output outputs/priority_list.csv --production-output outputs/production_plan.csv
```

If you only have inventory available:

```bash
python src/build_priority_list.py --inventory "path/to/distru-inventory-valuation-report.csv" --output outputs/priority_list.csv --production-output outputs/production_plan.csv
```

If you maintain a separate condensed source export (bulk flower/smalls/trim/distillate), use:

```bash
python src/build_priority_list.py --inventory "path/to/needs-packages.csv" --source-inventory "path/to/source-packages.csv" --output outputs/priority_list.csv --production-output outputs/production_plan.csv --top-n 25
```

If you need scheduling-ready output (requires testing status):

```bash
python src/build_priority_list.py --inventory "path/to/distru-inventory-valuation-report.csv" --output outputs/priority_list.csv --production-output outputs/production_plan.csv --testing-status inputs/testing_status.csv
```

Default scheduling output is capped at top 25 rows by priority rank. Override with:

```bash
python src/build_priority_list.py --inventory "path/to/distru-inventory-valuation-report.csv" --output outputs/priority_list.csv --production-output outputs/production_plan.csv --testing-status inputs/testing_status.csv --top-n 25
```

Split station queues are also generated automatically in `outputs/stations/`:

- `cart_queue.csv`
- `unit_queue.csv`
- `preroll_queue.csv`

You can change the folder with:

```bash
python src/build_priority_list.py --inventory "path/to/inventory.csv" --production-output outputs/production_plan.csv --station-output-dir outputs/stations --top-n 25
```

## Output columns

- Priority Rank
- Priority (`Critical`, `High`, `Medium`, `Low`)
- SKU / Name
- Available Qty / Incoming Qty / Threshold Min
- Units Sold / Avg Daily Sales / Days of Cover
- Recommended Reorder Qty
- Urgency Score

Additional file:

- `outputs/production_plan.csv` with suggested source material per need.

## Production constraints implemented

- Source inventory is restricted to `Bulk Flower` and `Bulk Smalls` for Unit-style needs.
- `Bulk Smalls` are treated as `14g`-only packaging targets (not `3.5g`).
- Source inventory is restricted to `Bulk Distillate` for Cart-style needs.
- Source inventory is restricted to `Trim` or `Preroll Material` for Preroll needs.
- Matching prefers same strain/flavor text; if none found it falls back to highest available valid source.
- Scheduling rows are included only when testing status is exactly `TEST PASSED`.
- For packages export, `Lab Testing State` is used automatically (`TestPassed` accepted).
- Items flagged as `tester`, `promo`, or `display` are excluded.

### Priority scope (needs only)

Priority rows are limited to retail-facing units and prerolls:

- `3.5g Jar`
- `3.5g Mylar`
- `14g Mylar`
- `1g Cart`
- `1g AIO Cart`
- all Pre-roll types (`1g Pre-Roll`, `5pk Pre-Roll`, etc.)

## Sales note

If a sales file is provided but does not contain SKU + quantity columns, the script automatically falls back to inventory-only prioritization.

## Rerun anytime

Drop in the latest two exports and run the same command. The logic is deterministic and designed for repeat use.

## Automated refresh (recommended)

Use this if Distru reports are emailed on a schedule and you want fully automatic updates:

1. Gmail filter labels reports (example label: `distru-auto`)
2. Apps Script saves latest attachments to Drive with fixed names:
	- `latest_needs.csv`
	- `latest_source.csv`
3. Mirror those files into:
	- `inputs/needs/latest_needs.csv`
	- `inputs/source/latest_source.csv`
4. Run local refresh with one command:

```bash
python src/run_latest_pipeline.py
```

5. (Optional) Apps Script imports generated CSVs into Google Sheets tabs.

See full setup in `config/automation_setup.md`.

## Local upload landing page

Use this when you want to manually upload a fresh test set from your machine and run the same pipeline locally.

1. Install dependency:

```bash
python -m pip install -r requirements.txt
```

2. Start the local page:

```bash
python web/app.py --port 5050
```

Or use one click on Windows:

```text
start_local_ui.bat
```

3. Open in browser:

```text
http://127.0.0.1:5050
```

4. Upload files and run:

- Inventory CSV (required)
- Sales CSV (optional)

Testing status is read from the inventory/packages export when available.

Downloads are exposed directly after each run from `outputs/` and `outputs/stations/`.

## Auto-upload watch mode (inventory + sales)

Use this when your email automation drops CSV files into a local folder and you want the pipeline to run automatically.

1. Start watcher (one click on Windows):

```text
start_auto_watch.bat
```

2. Drop CSV files into:

```text
inputs/auto_drop
```

3. Filename matching rules:

- Inventory file name includes one of: `inventory`, `packages`, `valuation`, `needs`
- Sales file name includes one of: `sales`, `sku`, `sold`

When a new inventory+sales pair is detected, outputs are regenerated automatically in `outputs/` and `outputs/stations/`.

### Recommended strict mode (for Gmail/Apps Script fixed names)

If Apps Script writes fixed names (`latest_inventory.csv` and `latest_sales.csv`), use strict mode:

```bash
python src/watch_inventory_sales.py --watch --interval 20 --strict-latest-names
```

The one-click launcher `start_auto_watch.bat` already uses this mode.

### Keep Drive syncing to local machine

1. Install Google Drive for desktop and sign in with the same Google account as Apps Script.
2. In Drive for desktop preferences, make sure your destination Drive folder is synced locally.
3. Copy or sync these two files into `inputs/auto_drop`:
	- `latest_inventory.csv`
	- `latest_sales.csv`
4. Keep `start_auto_watch.bat` running.

Watcher debug files:

- `outputs/auto_watch.log`
- `outputs/auto_watch_status.json`

Manual CLI option:

```bash
python src/watch_inventory_sales.py --watch --interval 20
```

## Public view-only hosting (for NFC station links)

This app can be hosted publicly in view-only mode so floor teams can scan NFC tags and open station pages.

### Deploy on Render

1. Push this repo to GitHub.
2. In Render, create a **Blueprint** service from the repo (uses `render.yaml`).
3. Deploy.

The service starts with:

- `VIEW_ONLY=true` (upload form disabled)
- stable station routes enabled.

### Stable station URLs for NFC

Replace `<your-domain>` with your Render URL:

- All stations: `https://<your-domain>/stations`
- Cart station: `https://<your-domain>/stations/cart`
- Unit station: `https://<your-domain>/stations/unit`
- Preroll station: `https://<your-domain>/stations/preroll`

### Important note about freshness

Hosted view pages only show the files available on the host filesystem. To keep them current, you need a sync/deploy step that updates hosted `outputs/` after each pipeline run.

## Direct Distru -> Live App (no laptop watcher)

Use this mode if you want Gmail/App Script to send reports straight to the hosted app, which immediately regenerates outputs.

### 1) Configure hosted env vars

- `VIEW_ONLY=true`
- `INGEST_TOKEN=<long-random-secret>`

### 2) Use Apps Script webhook sender

Script file: `config/apps_script_gmail_to_webhook.gs`

Set:

- `WEBHOOK_URL=https://<your-domain>/api/ingest`
- `INGEST_TOKEN` to the same server token

Run function:

- `sendLatestDistruReportsToLiveApp`

Then add a 2-hour trigger for that function.

### 3) Public NFC links

These stay stable and update after each successful ingest:

- `https://<your-domain>/stations`
- `https://<your-domain>/stations/cart`
- `https://<your-domain>/stations/unit`
- `https://<your-domain>/stations/preroll`

### Auto-publish hosted outputs after each run

If your hosting provider auto-deploys from your GitHub branch, you can publish new outputs automatically after each successful watcher run.

One-click launcher:

```text
start_auto_watch_publish.bat
```

What it does:

1. Watches for `latest_inventory.csv` and `latest_sales.csv`.
2. Runs the pipeline.
3. Runs `scripts/publish_outputs.py --push`.
4. Pushes updated `outputs/` to `origin/main`.
5. Hosting auto-redeploys and NFC links show new content.

Manual command:

```bash
python src/watch_inventory_sales.py --watch --interval 20 --strict-latest-names --post-run-command "python scripts/publish_outputs.py --push"
```
