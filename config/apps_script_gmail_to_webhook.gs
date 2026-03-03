const CONFIG = {
  LABEL_NAME: 'distru-auto',
  MAX_THREADS: 100,
  WEBHOOK_URL: 'https://YOUR-LIVE-DOMAIN/api/ingest',
  INGEST_TOKEN: 'REPLACE_WITH_INGEST_TOKEN',
};

function sendLatestDistruReportsToLiveApp() {
  const label = GmailApp.getUserLabelByName(CONFIG.LABEL_NAME);
  if (!label) {
    throw new Error(`Label not found: ${CONFIG.LABEL_NAME}`);
  }

  if (!CONFIG.WEBHOOK_URL || CONFIG.WEBHOOK_URL.indexOf('YOUR-LIVE-DOMAIN') !== -1) {
    throw new Error('Set CONFIG.WEBHOOK_URL to your live endpoint URL.');
  }
  if (!CONFIG.INGEST_TOKEN || CONFIG.INGEST_TOKEN.indexOf('REPLACE_WITH') !== -1) {
    throw new Error('Set CONFIG.INGEST_TOKEN to your server ingest token.');
  }

  const threads = label.getThreads(0, CONFIG.MAX_THREADS);
  let latestInventory = null;
  let latestSales = null;
  let scannedCsv = 0;

  for (const thread of threads) {
    for (const message of thread.getMessages()) {
      const msgDate = message.getDate();
      const subject = (message.getSubject() || '').toLowerCase();
      const attachments = message.getAttachments({ includeInlineImages: false });

      for (const attachment of attachments) {
        const name = (attachment.getName() || '').toLowerCase();
        if (!name.endsWith('.csv')) continue;

        scannedCsv += 1;
        const kind = classifyCsvType_(name, subject);
        if (!kind) continue;

        const candidate = {
          date: msgDate,
          name,
          blob: normalizeCsvBlob_(attachment.copyBlob()),
        };

        if (kind === 'inventory' && isNewer_(candidate, latestInventory)) {
          latestInventory = candidate;
        }

        if (kind === 'sales' && isNewer_(candidate, latestSales)) {
          latestSales = candidate;
        }
      }
    }
  }

  Logger.log(`Scanned CSV attachments: ${scannedCsv}`);

  if (!latestInventory) {
    throw new Error('No inventory CSV found in labeled emails.');
  }

  const formData = {
    inventory: latestInventory.blob.setName('latest_inventory.csv'),
  };
  if (latestSales) {
    formData.sales = latestSales.blob.setName('latest_sales.csv');
  }

  const response = UrlFetchApp.fetch(CONFIG.WEBHOOK_URL, {
    method: 'post',
    headers: {
      'X-Ingest-Token': CONFIG.INGEST_TOKEN,
    },
    payload: formData,
    muteHttpExceptions: true,
  });

  const code = response.getResponseCode();
  const body = response.getContentText();

  Logger.log(`Webhook response code: ${code}`);
  Logger.log(`Webhook response body: ${body}`);

  if (code < 200 || code >= 300) {
    throw new Error(`Webhook failed with status ${code}`);
  }
}

function classifyCsvType_(filename, subject) {
  const combined = `${String(filename || '').toLowerCase()} ${String(subject || '').toLowerCase()}`;

  const inventorySignals = ['inventory', 'inventory valuation', 'valuation', 'packages', 'needs'];
  const salesSignals = ['sales', 'sales by product', 'sku', 'sold'];

  if (inventorySignals.some(signal => combined.indexOf(signal) !== -1)) return 'inventory';
  if (salesSignals.some(signal => combined.indexOf(signal) !== -1)) return 'sales';
  return null;
}

function normalizeCsvBlob_(blob) {
  const content = blob.getDataAsString('UTF-8');
  return Utilities.newBlob(content, 'text/csv');
}

function isNewer_(candidate, current) {
  if (!current) return true;
  return candidate.date.getTime() > current.date.getTime();
}
