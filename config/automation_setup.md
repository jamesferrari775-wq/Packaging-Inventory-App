# Automation Setup (Gmail -> Drive -> Local Pipeline -> Google Sheets)

This setup gives you near-real-time refresh with minimal manual work.

## 1) Gmail label for Distru exports

1. In Gmail, create a label: `distru-auto`.
2. Create filters for your scheduled Distru report emails and apply that label.
3. Keep only the export emails in that label (easier parsing).

## 2) Apps Script: Gmail attachments to Drive

1. Create a Drive folder for raw exports.
2. Open script.google.com and create a project.
3. Paste code from `config/apps_script_gmail_to_drive.gs`.
4. Update constants at the top:
   - `LABEL_NAME`
   - `DESTINATION_FOLDER_ID`
5. Create a time-based trigger (every 2 hours) for `pullDistruCsvAttachments`.

This writes normalized files:
- `latest_needs.csv`
- `latest_source.csv`

## 3) Local folder handoff

Keep these local folders in this repo:
- `inputs/needs`
- `inputs/source`

Copy/sync latest Drive files into:
- `inputs/needs/latest_needs.csv`
- `inputs/source/latest_source.csv`

You can do this manually at first; later you can automate with Drive for Desktop or a sync job.

## 4) Run pipeline from newest files

From workspace root:

```bash
python src/run_latest_pipeline.py
```

This script automatically picks newest CSV from:
- `inputs/needs/*.csv`
- `inputs/source/*.csv`

and regenerates:
- `outputs/priority_list.csv`
- `outputs/production_plan.csv`
- `outputs/team_todo.csv`
- `outputs/stations/*.csv`
- `outputs/stations/*_station_tiles.html`

## 5) Schedule local refresh (Windows Task Scheduler)

Create a task that runs every 2 hours:

Program:

`C:\Users\admin\AppData\Local\Programs\Python\Python313\python.exe`

Arguments:

`src/run_latest_pipeline.py`

Start in:

`C:\dev\Packaging+Inventory Priority`

## 6) (Optional) Apps Script: Drive CSV to Google Sheet tabs

1. Create/open your Google Sheet dashboard.
2. Open Extensions -> Apps Script.
3. Paste code from `config/apps_script_drive_to_sheets.gs`.
4. Update:
   - `SOURCE_FOLDER_ID`
   - file names + tab names mapping
5. Add trigger (every 2 hours) for `refreshDashboardTabs`.

This keeps tabs synced from latest CSV outputs.

---

## MVP path (fastest)

If you want the fastest live setup:
1. Automate Gmail -> Drive first.
2. Manually drop files into `inputs/needs` and `inputs/source`.
3. Run `python src/run_latest_pipeline.py`.
4. Confirm outputs and tiles.
5. Add scheduler + Sheets sync after validation.
