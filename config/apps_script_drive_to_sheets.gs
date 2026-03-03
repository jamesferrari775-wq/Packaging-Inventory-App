const SOURCE_FOLDER_ID = 'REPLACE_WITH_DRIVE_FOLDER_ID';

const FILE_TAB_MAP = [
  { fileName: 'team_todo.csv', tabName: 'Team ToDo' },
  { fileName: 'cart_todo.csv', tabName: 'Cart ToDo' },
  { fileName: 'unit_todo.csv', tabName: 'Unit ToDo' },
  { fileName: 'preroll_todo.csv', tabName: 'Preroll ToDo' },
  { fileName: 'priority_list.csv', tabName: 'Priority List' },
  { fileName: 'production_plan.csv', tabName: 'Production Plan' },
];

function refreshDashboardTabs() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const folder = DriveApp.getFolderById(SOURCE_FOLDER_ID);

  for (const entry of FILE_TAB_MAP) {
    const file = newestFileByName(folder, entry.fileName);
    if (!file) continue;

    const csv = file.getBlob().getDataAsString('UTF-8');
    const rows = Utilities.parseCsv(csv);
    if (!rows || rows.length === 0) continue;

    let sheet = ss.getSheetByName(entry.tabName);
    if (!sheet) {
      sheet = ss.insertSheet(entry.tabName);
    }

    sheet.clearContents();
    sheet.getRange(1, 1, rows.length, rows[0].length).setValues(rows);
  }
}

function newestFileByName(folder, fileName) {
  const files = folder.getFilesByName(fileName);
  let newest = null;

  while (files.hasNext()) {
    const file = files.next();
    if (!newest || file.getLastUpdated() > newest.getLastUpdated()) {
      newest = file;
    }
  }

  return newest;
}
