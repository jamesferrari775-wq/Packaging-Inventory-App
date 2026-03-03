const SOURCE_FOLDER_ID = 'REPLACE_WITH_DRIVE_FOLDER_ID';
const TILE_TOP_N = 30;

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

  buildStationTileTabs(ss, TILE_TOP_N);
}


function testDriveLinksAndSchedulePopulate() {
  if (!SOURCE_FOLDER_ID || SOURCE_FOLDER_ID === 'REPLACE_WITH_DRIVE_FOLDER_ID') {
    throw new Error('Set SOURCE_FOLDER_ID before running this test.');
  }

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const folder = DriveApp.getFolderById(SOURCE_FOLDER_ID);
  Logger.log('Drive test start: folder="%s" id=%s', folder.getName(), folder.getId());

  for (const entry of FILE_TAB_MAP) {
    const file = newestFileByName(folder, entry.fileName);
    if (!file) {
      Logger.log('[MISSING] %s -> expected tab "%s"', entry.fileName, entry.tabName);
      continue;
    }

    const csv = file.getBlob().getDataAsString('UTF-8');
    const rows = Utilities.parseCsv(csv);
    const rowCount = rows ? rows.length : 0;

    Logger.log(
      '[FOUND] %s -> tab "%s" | rows=%s | updated=%s | url=%s',
      entry.fileName,
      entry.tabName,
      rowCount,
      file.getLastUpdated(),
      file.getUrl()
    );
  }

  refreshDashboardTabs();

  const verifyTabs = FILE_TAB_MAP.map(e => e.tabName)
    .concat(['All Station Tiles', 'Cart Tiles', 'Unit Tiles', 'Preroll Tiles']);

  for (const tabName of verifyTabs) {
    const sheet = ss.getSheetByName(tabName);
    if (!sheet) {
      Logger.log('[TAB MISSING] %s', tabName);
      continue;
    }

    Logger.log('[TAB OK] %s -> rows=%s cols=%s', tabName, sheet.getLastRow(), sheet.getLastColumn());
  }

  Logger.log('Drive test complete.');
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


function buildStationTileTabs(ss, topN) {
  const source = ss.getSheetByName('Team ToDo');
  if (!source) return;

  const data = source.getDataRange().getValues();
  if (!data || data.length < 2) return;

  const header = data[0].map(v => String(v));
  const rows = data.slice(1);
  const idx = indexMap_(header);

  if (idx.station === -1 || idx.needName === -1) return;

  const stations = ['Cart', 'Unit', 'Preroll'];
  const grouped = {
    All: rows.filter(r => String(r[idx.needName] || '').trim() !== ''),
    Cart: rows.filter(r => String(r[idx.station] || '') === 'Cart'),
    Unit: rows.filter(r => String(r[idx.station] || '') === 'Unit'),
    Preroll: rows.filter(r => String(r[idx.station] || '') === 'Preroll'),
  };

  renderTileSheet_(ss, 'All Station Tiles', grouped.All, idx, topN);
  for (const station of stations) {
    renderTileSheet_(ss, `${station} Tiles`, grouped[station], idx, topN);
  }
}


function renderTileSheet_(ss, sheetName, rows, idx, topN) {
  const sheet = upsertSheet_(ss, sheetName);
  sheet.clear({ contentsOnly: false });

  const title = `${sheetName} (Top ${topN})`;
  sheet.getRange('A1').setValue(title).setFontSize(16).setFontWeight('bold');
  sheet.setFrozenRows(1);

  const subset = rows.slice(0, topN);
  if (subset.length === 0) {
    sheet.getRange('A3').setValue('No rows available.');
    return;
  }

  const cardsPerRow = 2;
  const cardWidth = 5;
  const cardHeight = 8;

  for (let i = 0; i < subset.length; i++) {
    const row = subset[i];
    const colBlock = i % cardsPerRow;
    const rowBlock = Math.floor(i / cardsPerRow);
    const startRow = 3 + (rowBlock * cardHeight);
    const startCol = 1 + (colBlock * cardWidth);

    const rank = safe_(row, idx.todoRank) || String(i + 1);
    const station = safe_(row, idx.station);
    const needName = safe_(row, idx.needName);
    const sku = safe_(row, idx.needSku);
    const nextAction = safe_(row, idx.nextAction);
    const sourceName = safe_(row, idx.sourceName);
    const breakdown = safe_(row, idx.packagedBreakdown);
    const missingTypes = safe_(row, idx.missingTypes);

    const lines = [
      `#${rank} • ${station}`,
      needName,
      `SKU: ${sku}`,
      `Source: ${sourceName}`,
      `Inventory: ${breakdown}`,
      `Missing: ${missingTypes}`,
      `Next: ${nextAction}`,
    ];

    const range = sheet.getRange(startRow, startCol, cardHeight - 1, cardWidth - 1);
    range.merge();
    range
      .setValue(lines.join('\n'))
      .setWrap(true)
      .setVerticalAlignment('top')
      .setHorizontalAlignment('left')
      .setBackground('#f8fafc')
      .setBorder(true, true, true, true, true, true, '#d1d5db', SpreadsheetApp.BorderStyle.SOLID);

    sheet.setRowHeight(startRow, 120);
  }

  sheet.autoResizeColumns(1, cardsPerRow * cardWidth);
}


function indexMap_(header) {
  const lower = header.map(h => String(h).toLowerCase());
  const find = name => lower.indexOf(name.toLowerCase());

  return {
    station: find('Station'),
    todoRank: find('ToDo Rank'),
    needName: find('Need Name'),
    needSku: find('Need SKU'),
    nextAction: find('Next Action'),
    sourceName: find('Suggested Source Name'),
    packagedBreakdown: find('Packaged Unit Breakdown'),
    missingTypes: find('Missing Required Types'),
  };
}


function upsertSheet_(ss, name) {
  let sheet = ss.getSheetByName(name);
  if (!sheet) sheet = ss.insertSheet(name);
  return sheet;
}


function safe_(row, idx) {
  if (idx === -1 || idx >= row.length) return '';
  return String(row[idx] == null ? '' : row[idx]);
}
