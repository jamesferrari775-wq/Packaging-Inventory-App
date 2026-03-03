const CONFIG = {
  LABEL_NAME: 'distru-auto',
  DESTINATION_FOLDER: 'https://drive.google.com/drive/folders/1pnl4vtQVQGDY7eDcPt-EoI_NdEoUS2m8?usp=drive_link',
  DESTINATION_FOLDER_NAME_FALLBACK: 'Distru Auto Exports',
  MAX_THREADS: 100,
  INVENTORY_TARGET_NAME: 'latest_inventory.csv',
  SALES_TARGET_NAME: 'latest_sales.csv',
};

function pullDistruInventorySalesCsvs() {
  ingestInventorySales({ dryRun: false });
}

function dryRunDistruInventorySalesCsvs() {
  ingestInventorySales({ dryRun: true });
}

function validateDistruSetup() {
  const label = getRequiredLabel_();
  const folder = resolveDestinationFolder_();

  Logger.log(`Label found: ${label.getName()}`);
  Logger.log(`Destination folder: ${folder.getName()} (${folder.getId()})`);
}

function testDriveFolderAccess() {
  const folder = resolveDestinationFolder_();
  Logger.log(`Folder OK: ${folder.getName()} (${folder.getId()})`);
}

function ingestInventorySales(options) {
  const dryRun = Boolean(options && options.dryRun);
  const label = getRequiredLabel_();
  const folder = resolveDestinationFolder_();
  const threads = label.getThreads(0, CONFIG.MAX_THREADS);

  const state = {
    scannedThreads: threads.length,
    scannedCsvAttachments: 0,
    latestInventory: null,
    latestSales: null,
  };

  for (const thread of threads) {
    const messages = thread.getMessages();

    for (const message of messages) {
      const messageDate = message.getDate();
      const subject = (message.getSubject() || '').toLowerCase();
      const attachments = message.getAttachments({ includeInlineImages: false });

      for (const attachment of attachments) {
        const filename = String(attachment.getName() || '').toLowerCase();
        if (!filename.endsWith('.csv')) {
          continue;
        }

        state.scannedCsvAttachments += 1;
        const kind = classifyCsvType_(filename, subject);
        if (!kind) {
          continue;
        }

        const blob = normalizeCsvBlob_(attachment.copyBlob());
        const candidate = {
          date: messageDate,
          filename,
          subject,
          blob,
        };

        if (kind === 'inventory' && isNewerCandidate_(candidate, state.latestInventory)) {
          state.latestInventory = candidate;
        }

        if (kind === 'sales' && isNewerCandidate_(candidate, state.latestSales)) {
          state.latestSales = candidate;
        }
      }
    }
  }

  Logger.log(`Scanned threads: ${state.scannedThreads}`);
  Logger.log(`Scanned CSV attachments: ${state.scannedCsvAttachments}`);

  if (state.latestInventory) {
    Logger.log(`Latest inventory: ${state.latestInventory.filename} @ ${state.latestInventory.date}`);
  } else {
    Logger.log('No inventory CSV match found.');
  }

  if (state.latestSales) {
    Logger.log(`Latest sales: ${state.latestSales.filename} @ ${state.latestSales.date}`);
  } else {
    Logger.log('No sales CSV match found.');
  }

  if (dryRun) {
    Logger.log('Dry-run complete. No files were written.');
    return;
  }

  if (state.latestInventory) {
    upsertFile_(folder, CONFIG.INVENTORY_TARGET_NAME, state.latestInventory.blob);
    Logger.log(`Wrote ${CONFIG.INVENTORY_TARGET_NAME}`);
  }

  if (state.latestSales) {
    upsertFile_(folder, CONFIG.SALES_TARGET_NAME, state.latestSales.blob);
    Logger.log(`Wrote ${CONFIG.SALES_TARGET_NAME}`);
  }

  if (!state.latestInventory && !state.latestSales) {
    Logger.log('No files written because no matching CSV attachments were found.');
  }
}

function getRequiredLabel_() {
  const label = GmailApp.getUserLabelByName(CONFIG.LABEL_NAME);
  if (!label) {
    throw new Error(`Label not found: ${CONFIG.LABEL_NAME}`);
  }
  return label;
}

function resolveDestinationFolder_() {
  const raw = String(CONFIG.DESTINATION_FOLDER || '').trim();

  // 1) Try explicit folder ID or full URL
  const idMatch = raw.match(/[\w-]{25,}/);
  if (idMatch && idMatch[0]) {
    const folderId = idMatch[0];
    try {
      return DriveApp.getFolderById(folderId);
    } catch (err) {
      Logger.log(`Could not open folder by ID (${folderId}). Falling back to folder name.`);
    }
  }

  // 2) Try folder name fallback
  const fallbackName = String(CONFIG.DESTINATION_FOLDER_NAME_FALLBACK || '').trim();
  if (!fallbackName) {
    throw new Error('No usable destination folder. Set CONFIG.DESTINATION_FOLDER or DESTINATION_FOLDER_NAME_FALLBACK.');
  }

  const existing = DriveApp.getFoldersByName(fallbackName);
  if (existing.hasNext()) {
    return existing.next();
  }

  // 3) Create fallback folder in My Drive
  Logger.log(`Creating fallback folder in My Drive: ${fallbackName}`);
  return DriveApp.createFolder(fallbackName);
}

function classifyCsvType_(filename, subject) {
  const combined = `${String(filename || '').toLowerCase()} ${String(subject || '').toLowerCase()}`;

  const inventorySignals = [
    'inventory',
    'inventory valuation',
    'valuation',
    'packages',
    'needs',
  ];

  const salesSignals = [
    'sales',
    'sales by product',
    'sku',
    'sold',
  ];

  if (inventorySignals.some(signal => combined.includes(signal))) {
    return 'inventory';
  }

  if (salesSignals.some(signal => combined.includes(signal))) {
    return 'sales';
  }

  return null;
}

function normalizeCsvBlob_(blob) {
  const content = blob.getDataAsString('UTF-8');
  return Utilities.newBlob(content, 'text/csv');
}

function isNewerCandidate_(candidate, current) {
  if (!current) {
    return true;
  }
  return candidate.date.getTime() > current.date.getTime();
}

function upsertFile_(folder, targetName, blob) {
  const existing = folder.getFilesByName(targetName);
  while (existing.hasNext()) {
    existing.next().setTrashed(true);
  }

  blob.setName(targetName);
  folder.createFile(blob);
}
