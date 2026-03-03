const CONFIG = {
  LABEL_NAME: 'distru-auto',
  MAX_THREADS: 100,
  WEBHOOK_URL: 'https://packaging-inventory-app.onrender.com/api/ingest',
  INGEST_TOKEN: 'REPLACE_WITH_INGEST_TOKEN',
  REQUEST_TIMEOUT_SECONDS: 120,
};

function sendLatestDistruReportsToLiveApp() {
  Logger.log('=== sendLatestDistruReportsToLiveApp started ===');
  validateConfig_();

  const label = GmailApp.getUserLabelByName(CONFIG.LABEL_NAME);
  if (!label) {
    throw new Error(`Label not found: ${CONFIG.LABEL_NAME}`);
  }

  const threads = label.getThreads(0, CONFIG.MAX_THREADS);
  Logger.log(`Threads scanned: ${threads.length}`);

  let latestInventory = null;
  let latestSales = null;
  let scannedCsvCount = 0;

  for (const thread of threads) {
    const messages = thread.getMessages();
    for (const message of messages) {
      const msgDate = message.getDate();
      const subject = String(message.getSubject() || '').toLowerCase();
      const attachments = message.getAttachments({ includeInlineImages: false });

      for (const attachment of attachments) {
        const fileName = String(attachment.getName() || '').toLowerCase();
        if (!fileName.endsWith('.csv')) {
          continue;
        }

        scannedCsvCount += 1;
        const type = classifyCsvType_(fileName, subject);
        if (!type) {
          continue;
        }

        const candidate = {
          date: msgDate,
          name: fileName,
          subject,
          blob: normalizeCsvBlob_(attachment.copyBlob()),
        };

        if (type === 'inventory' && isNewer_(candidate, latestInventory)) {
          latestInventory = candidate;
        }
        if (type === 'sales' && isNewer_(candidate, latestSales)) {
          latestSales = candidate;
        }
      }
    }
  }

  Logger.log(`Scanned CSV attachments: ${scannedCsvCount}`);
  Logger.log(
    latestInventory
      ? `Latest inventory: ${latestInventory.name} @ ${latestInventory.date}`
      : 'Latest inventory: NOT FOUND'
  );
  Logger.log(latestSales ? `Latest sales: ${latestSales.name} @ ${latestSales.date}` : 'Latest sales: NOT FOUND');

  if (!latestInventory) {
    throw new Error('No inventory CSV found in labeled emails. Ensure latest inventory report is labeled distru-auto.');
  }

  const payload = {
    inventory: latestInventory.blob.setName('latest_inventory.csv'),
    ingest_token: CONFIG.INGEST_TOKEN,
  };
  if (latestSales) {
    payload.sales = latestSales.blob.setName('latest_sales.csv');
  }

  const response = UrlFetchApp.fetch(CONFIG.WEBHOOK_URL, {
    method: 'post',
    headers: {
      'X-Ingest-Token': CONFIG.INGEST_TOKEN,
      Authorization: `Bearer ${CONFIG.INGEST_TOKEN}`,
    },
    payload,
    muteHttpExceptions: true,
    followRedirects: true,
  });

  const status = response.getResponseCode();
  const body = response.getContentText();
  Logger.log(`Webhook response code: ${status}`);
  Logger.log(`Webhook response body: ${body}`);

  if (status < 200 || status >= 300) {
    throw new Error(`Webhook failed with status ${status}. Response: ${body}`);
  }

  Logger.log('=== sendLatestDistruReportsToLiveApp completed successfully ===');
}

function testWebhookConfigOnly() {
  validateConfig_();
  Logger.log('Config looks valid.');
  Logger.log(`Webhook URL: ${CONFIG.WEBHOOK_URL}`);
  Logger.log(`Label: ${CONFIG.LABEL_NAME}`);
}

function validateConfig_() {
  if (!CONFIG.WEBHOOK_URL || CONFIG.WEBHOOK_URL.indexOf('onrender.com') === -1) {
    throw new Error('CONFIG.WEBHOOK_URL is not set to your Render ingest URL.');
  }
  if (!CONFIG.INGEST_TOKEN || CONFIG.INGEST_TOKEN.indexOf('REPLACE_WITH') !== -1) {
    throw new Error('Set CONFIG.INGEST_TOKEN to match Render env var INGEST_TOKEN.');
  }
  if (!CONFIG.LABEL_NAME) {
    throw new Error('CONFIG.LABEL_NAME is required.');
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
