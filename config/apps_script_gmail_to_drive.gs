const LABEL_NAME = 'distru-auto';
const DESTINATION_FOLDER_ID = 'REPLACE_WITH_DRIVE_FOLDER_ID';

function pullDistruCsvAttachments() {
  const label = GmailApp.getUserLabelByName(LABEL_NAME);
  if (!label) {
    throw new Error(`Label not found: ${LABEL_NAME}`);
  }

  const folder = DriveApp.getFolderById(DESTINATION_FOLDER_ID);
  const threads = label.getThreads(0, 50);

  let latestNeeds = null;
  let latestSource = null;

  for (const thread of threads) {
    const messages = thread.getMessages();
    for (const message of messages) {
      const msgDate = message.getDate();
      const attachments = message.getAttachments({ includeInlineImages: false });

      for (const attachment of attachments) {
        const originalName = (attachment.getName() || '').toLowerCase();
        if (!originalName.endsWith('.csv')) continue;

        const normalized = normalizeBlob(attachment.copyBlob());
        const kind = classifyCsvType(originalName);

        if (kind === 'needs') {
          if (!latestNeeds || msgDate > latestNeeds.date) {
            latestNeeds = { date: msgDate, blob: normalized, name: originalName };
          }
        } else if (kind === 'source') {
          if (!latestSource || msgDate > latestSource.date) {
            latestSource = { date: msgDate, blob: normalized, name: originalName };
          }
        }
      }
    }
  }

  if (latestNeeds) {
    upsertFile(folder, 'latest_needs.csv', latestNeeds.blob);
  }
  if (latestSource) {
    upsertFile(folder, 'latest_source.csv', latestSource.blob);
  }
}

function classifyCsvType(filename) {
  const name = filename.toLowerCase();
  if (name.includes('source') || name.includes('bulk') || name.includes('trim') || name.includes('distillate')) {
    return 'source';
  }
  return 'needs';
}

function normalizeBlob(blob) {
  const content = blob.getDataAsString('UTF-8');
  return Utilities.newBlob(content, 'text/csv');
}

function upsertFile(folder, targetName, blob) {
  const existing = folder.getFilesByName(targetName);
  while (existing.hasNext()) {
    existing.next().setTrashed(true);
  }
  blob.setName(targetName);
  folder.createFile(blob);
}
