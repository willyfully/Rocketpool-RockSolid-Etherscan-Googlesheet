// =============================================================================
// RockSolid-Etherscan-V2.gs.js
// =============================================================================
//
// Clean two-phase, two-tier architecture:
//
//   TIER 1 — Raw event sheets (exact blockchain event data, append-only):
//     RP_evt_BalancesUpdated    — rocketNetworkBalances BalancesUpdated events (v1–v4)
//     RS_evt_TotalAssetsUpdated — rock.rETH oracle update events
//     RS_evt_SettleRedeem       — rock.rETH settlement events
//     RS_evt_Transfer           — rock.rETH ERC-20 Transfer events (mints, burns, peer-to-peer)
//
//   TIER 2 — Derived sheets (computed from raw data, zero API calls):
//     rETH_ratio             — rETH/ETH ratio history + APY
//     rock.rETH_ratio        — rock.rETH running state, ratios, APY
//     wallets_all            — all non-fee wallet mints & burns with per-wallet position tracking
//     wallets_summary        — one summary row per wallet (current position + P&L)
//     wallet_<addr>          — personal vault position for the single tracked wallet
//     Dashboard              — summary KPIs + charts
//
// Phase 1 — Import (API calls, writes only to raw event sheets):
//   importBalancesUpdated()  importRockSolidVaultEvents()  importAll()
//
// Phase 2 — Rebuild (no API calls, reads raw → writes derived sheets):
//   rebuildRethRatio()  rebuildRockRatio()
//   rebuildAllWallets()  rebuildWalletsSummary()  rebuildWallet()  rebuildAll()
//
// Optional auto-update:
//   installHourlyTrigger() — runs importAll+rebuildAll every hour, headless.
//   removeAutoUpdateTrigger() / showTriggerStatus()
//
// Cursor strategy:
//   All raw_* sheets: cursor = last row's (block_num, log_idx).
// =============================================================================

// ── Event topic0 hashes ───────────────────────────────────────────────────────
const TOPICS = {
  BALANCES_UPDATED_V1V2: '0x7bbbb137fdad433d6168b1c75c714c72b8abe8d07460f0c0b433063e7bf1f394',
  BALANCES_UPDATED_V3V4: '0xdd27295717c4fbd48b1840f846e18be6f0b7bd6b55608e697e53b15848cecdf9',
  TOTAL_ASSETS_UPDATED:  '0xf306601d1bd9ff6895ca817f568f68463b269e4b0cf4710e5f1003592ad29f5c',
  SETTLE_REDEEM:         '0xa8fe241e26fead168e608ab85aa4e059a34552bad0fc6d98961122cb5a0abefd',
  TRANSFER:              '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
};

// ── Contract addresses (all lowercase for direct string comparison against
//    topic-decoded addresses, which are always lowercased by topicToAddr_) ───
const ROCK_RETH_PROXY = '0x936facdf10c8c36294e7b9d28345255539d81bc7';

const RNB_CONTRACTS = [
  { address: '0x138313f102ce9a0662f826fca977e3ab4d6e5539', version: 'v1', topic: TOPICS.BALANCES_UPDATED_V1V2 },
  { address: '0x07fcabcbe4ff0d80c2b1eb42855c0131b6cba2f4', version: 'v2', topic: TOPICS.BALANCES_UPDATED_V1V2 },
  { address: '0x6cc65bf618f55ce2433f9d8d827fc44117d81399', version: 'v3', topic: TOPICS.BALANCES_UPDATED_V3V4 },
  { address: '0x1d9f14c6bfd8358b589964bad8665add248e9473', version: 'v4', topic: TOPICS.BALANCES_UPDATED_V3V4 },
];

const FEE_RECEIVER_ADDRESSES = new Set([
  '0xe841d56c504c55693a098948bd3b74f4e01e593a',
  '0x9a408dc07820d810331a17da5e3a737fa2540d4b',
]);

// Redemptions are NOT direct burns (to=0x0). Instead, users transfer their
// rock.rETH to this silo contract, which then performs the actual ERC-20 burn
// (silo→0x0) internally. We treat user→SILO transfers as "Burn" events.
const SILO_CONTRACT = '0x81cc01446cbd882e31d054fa89e5f4439484ec0a';

const ZERO_ADDR  = '0x' + '0'.repeat(40);
const ZERO_TOPIC = '0x' + '0'.repeat(64);

// ── Etherscan tunables ────────────────────────────────────────────────────────
const ETHERSCAN_PAGE_SIZE = 1000;
const ETHERSCAN_MAX_PAGES = 20;
const ETHERSCAN_SLEEP_MS  = 200;

// ── Auto-update trigger ───────────────────────────────────────────────────────
const TRIGGER_HANDLER_FN = 'triggeredUpdate';
const TRIGGER_HOURS      = 1; // Apps Script permits: 1, 2, 4, 6, 8, or 12

// ── Sheet names ──────────────────────────────────────────────────────────────
const SHEET_RP_EVT_BALANCES = 'RP_evt_BalancesUpdated';
const SHEET_RS_EVT_TA       = 'RS_evt_TotalAssetsUpdated';
const SHEET_RS_EVT_SR       = 'RS_evt_SettleRedeem';
const SHEET_RS_EVT_TRANSFER = 'RS_evt_Transfer';
const SHEET_RETH_RATIO      = 'rETH_ratio';
const SHEET_ROCK_RATIO      = 'rock.rETH_ratio';
const SHEET_WALLETS_ALL     = 'wallets_all';
const SHEET_WALLETS_SUMMARY = 'wallets_summary';
const SHEET_DASHBOARD       = 'Dashboard';
const SHEET_CHART_DATA      = '_chart_data';

// ── Raw sheet column headers ─────────────────────────────────────────────────
const RAW_BALANCES_HEADERS = [
  'tx_hash', 'block_num', 'log_idx', 'block_timestamp',
  'version', 'total_eth', 'staking_eth', 'reth_supply',
];
const RAW_TA_HEADERS = [
  'tx_hash', 'block_num', 'log_idx', 'block_timestamp', 'total_assets',
];
const RAW_SR_HEADERS = [
  'tx_hash', 'block_num', 'log_idx', 'block_timestamp',
  'epoch_id', 'settled_id',
  'total_assets', 'total_supply', 'assets_withdrawed', 'shares_burned',
];
const RAW_TRANSFER_HEADERS = [
  'tx_hash', 'block_num', 'log_idx', 'block_timestamp', 'from_addr', 'to_addr', 'value', 'value_wei',
];

// ── BigInt / format constants ────────────────────────────────────────────────
// Apps Script V8 supports BigInt but NOT `123n` literal syntax, so we always
// construct via BigInt(...) and use BIG_ZERO for the zero literal.
const WEI_PER_ETH_NUM = 1e18;
const WEI_PER_ETH_BI  = BigInt('1000000000000000000');
const BIG_ZERO        = BigInt(0);

const FMT_DATE = 'dd/mm/yyyy hh:mm:ss';
const FMT_18   = '0.000000000000000000';

// =============================================================================
// Menu
// =============================================================================

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Rocket Pool')
    .addItem('Set Etherscan API key…',              'promptSetApiKey')
    .addItem('Set tracked wallet address…',         'promptSetWalletAddress')
    .addSeparator()
    .addItem('Import all (API calls)',              'importAll')
    .addItem('Import: Rocket Pool events',          'importBalancesUpdated')
    .addItem('Import: RockSolid Vault events',      'importRockSolidVaultEvents')
    .addSeparator()
    .addItem('Rebuild all derived sheets (no API)', 'rebuildAll')
    .addItem('Rebuild: rETH ratio',                 'rebuildRethRatio')
    .addItem('Rebuild: rock.rETH ratio',            'rebuildRockRatio')
    .addItem('Rebuild: all wallets sheet',          'rebuildAllWallets')
    .addItem('Rebuild: wallets summary',            'rebuildWalletsSummary')
    .addItem('Rebuild: wallet sheet (tracked)',     'rebuildWallet')
    .addSeparator()
    .addItem('Refresh Dashboard',                   'refreshDashboard')
    .addSeparator()
    .addItem('Enable hourly auto-update',           'installHourlyTrigger')
    .addItem('Disable auto-update',                 'removeAutoUpdateTrigger')
    .addItem('Auto-update status…',                 'showTriggerStatus')
    .addToUi();
}

// =============================================================================
// UI helpers
// =============================================================================

// Safe alert: silently no-ops when no UI is attached (e.g. time-based triggers).
function tryAlert_(msg) {
  try { SpreadsheetApp.getUi().alert(msg); }
  catch (e) { /* no UI context — headless run */ }
}

// =============================================================================
// Auto-update trigger
// =============================================================================

function installHourlyTrigger() {
  removeAutoUpdateTrigger_(true); // never double-install
  ScriptApp.newTrigger(TRIGGER_HANDLER_FN)
    .timeBased()
    .everyHours(TRIGGER_HOURS)
    .create();
  tryAlert_('Auto-update installed: runs every ' + TRIGGER_HOURS + ' hour(s).\n' +
            'Logs: Extensions → Apps Script → Executions.');
}

function removeAutoUpdateTrigger() {
  removeAutoUpdateTrigger_(false);
}

function removeAutoUpdateTrigger_(silent) {
  let removed = 0;
  for (const t of ScriptApp.getProjectTriggers()) {
    if (t.getHandlerFunction() === TRIGGER_HANDLER_FN) {
      ScriptApp.deleteTrigger(t);
      removed++;
    }
  }
  if (!silent) tryAlert_('Removed ' + removed + ' auto-update trigger(s).');
}

function showTriggerStatus() {
  const count = ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === TRIGGER_HANDLER_FN).length;
  tryAlert_(count === 0
    ? 'Auto-update: disabled.'
    : 'Auto-update: ENABLED (' + count + ' trigger' + (count > 1 ? 's' : '') +
      ', every ' + TRIGGER_HOURS + 'h).');
}

// Time-driven handler: runs silently. No UI, no alerts.
// Errors are logged AND re-thrown so they show up in the Executions panel and
// trigger Apps Script's failure-notification email.
function triggeredUpdate() {
  const t0 = Date.now();
  try {
    const results = {
      balances:  importBalancesUpdated(true),
      base:      importRockBaseEvents(true),
      transfers: importTransfers(true),
    };
    rebuildAll(true);
    Logger.log('triggeredUpdate OK in ' + Math.round((Date.now() - t0) / 1000) +
      's: ' + JSON.stringify(results));
    if (Object.values(results).some(r => r === null)) {
      throw new Error('One or more import steps failed silently (see earlier log entries).');
    }
  } catch (e) {
    Logger.log('triggeredUpdate FAILED: ' + e.message + '\n' + (e.stack || ''));
    throw e;
  }
}

// =============================================================================
// API key and wallet address
// =============================================================================

function promptSetApiKey() {
  const ui = SpreadsheetApp.getUi();
  const result = ui.prompt(
    'Etherscan API key',
    'Paste your key. Stored in User Properties (not visible in code).',
    ui.ButtonSet.OK_CANCEL
  );
  if (result.getSelectedButton() !== ui.Button.OK) return;
  const key = result.getResponseText().trim();
  if (!key) { ui.alert('Key cannot be empty.'); return; }
  PropertiesService.getUserProperties().setProperty('ETHERSCAN_API_KEY', key);
  ui.alert('API key saved.');
}

function getApiKey_() {
  const key = PropertiesService.getUserProperties().getProperty('ETHERSCAN_API_KEY');
  if (!key) throw new Error('ETHERSCAN_API_KEY not set. Use "Rocket Pool → Set Etherscan API key…".');
  return key;
}

function promptSetWalletAddress() {
  const ui = SpreadsheetApp.getUi();
  const result = ui.prompt('Wallet address', 'Enter the Ethereum address to track (0x…).', ui.ButtonSet.OK_CANCEL);
  if (result.getSelectedButton() !== ui.Button.OK) return;
  const addr = result.getResponseText().trim().toLowerCase();
  if (!/^0x[0-9a-f]{40}$/.test(addr)) { ui.alert('Invalid address.'); return; }
  PropertiesService.getUserProperties().setProperty('TRACKED_WALLET', addr);
  ui.alert('Wallet saved: ' + addr);
}

function getWalletAddress_() {
  const addr = PropertiesService.getUserProperties().getProperty('TRACKED_WALLET');
  if (!addr) throw new Error('TRACKED_WALLET not set. Use "Rocket Pool → Set tracked wallet address…".');
  return addr;
}

// Returns null instead of throwing when no wallet is configured.
function getWalletOptional_() {
  return PropertiesService.getUserProperties().getProperty('TRACKED_WALLET') || null;
}

// =============================================================================
// Etherscan getLogs helper
// =============================================================================

// Returns { logs, truncated } — `truncated` is true when the page limit was hit
// and more results may exist. Throttling is handled here: every HTTP request
// (including the first of each call) is preceded by ETHERSCAN_SLEEP_MS, so
// back-to-back calls across contracts stay under the rate limit without any
// caller-side sleep.
function etherscanGetLogs_(address, topic0, fromBlock, apiKey, extraParams) {
  const baseUrl = 'https://api.etherscan.io/v2/api?chainid=1&module=logs&action=getLogs' +
    '&address=' + address +
    '&topic0=' + topic0 +
    '&fromBlock=' + fromBlock +
    '&toBlock=latest' +
    '&offset=' + ETHERSCAN_PAGE_SIZE +
    '&apikey=' + apiKey;

  let extraStr = '';
  if (extraParams) {
    for (const k in extraParams) extraStr += '&' + k + '=' + extraParams[k];
  }

  const logs = [];
  let truncated = false;
  for (let page = 1; page <= ETHERSCAN_MAX_PAGES; page++) {
    Utilities.sleep(ETHERSCAN_SLEEP_MS);

    const url      = baseUrl + '&page=' + page + extraStr;
    const response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    const code     = response.getResponseCode();
    if (code !== 200) throw new Error('Etherscan HTTP ' + code + ': ' + response.getContentText().slice(0, 200));

    const json = JSON.parse(response.getContentText());
    if (json.status !== '1') {
      if (json.message === 'No records found') break;
      throw new Error('Etherscan: ' + json.message + ' — ' + JSON.stringify(json.result).slice(0, 200));
    }

    const batch = json.result || [];
    for (const log of batch) logs.push(log);

    if (batch.length < ETHERSCAN_PAGE_SIZE) break;

    if (page === ETHERSCAN_MAX_PAGES) {
      truncated = true;
      Logger.log('etherscanGetLogs_: MAX_PAGES reached — address=' + address + ' fromBlock=' + fromBlock);
    }
  }
  return { logs, truncated };
}

// =============================================================================
// ABI / address helpers
// =============================================================================

function hexToInt_(hex)      { return parseInt(hex, 16); }
function wordToEth_(w)       { return Number(BigInt('0x' + w)) / WEI_PER_ETH_NUM; }
function addrToTopic_(addr)  { return '0x' + '0'.repeat(24) + addr.slice(2).toLowerCase(); }
function normalizeAddr_(a)   { return a ? String(a).toLowerCase() : a; }

function dataWords_(hexData, n) {
  const s = hexData.startsWith('0x') ? hexData.slice(2) : hexData;
  const words = [];
  for (let i = 0; i < n; i++) words.push(s.slice(i * 64, (i + 1) * 64));
  return words;
}

function topicToAddr_(topic) {
  return '0x' + (topic.startsWith('0x') ? topic.slice(2) : topic).slice(24).toLowerCase();
}

// =============================================================================
// Raw sheet helpers
// =============================================================================

// Returns the sheet, creating it with bold headers + a basic filter if it doesn't exist.
// dateColIdx (1-indexed) gets dd/mm/yyyy hh:mm:ss formatting.
// valueColIdxs is an array of 1-indexed column numbers to format as 18-decimal.
// textColIdxs is an array of 1-indexed column numbers to format as plain text
// (for BigInt strings that must not be coerced to float).
function ensureRawSheet_(name, headers, dateColIdx, valueColIdxs, textColIdxs) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(name);
  if (sheet) return sheet;

  sheet = ss.insertSheet(name);
  sheet.appendRow(headers);
  sheet.getRange(1, 1, 1, headers.length).setFontWeight('bold');
  sheet.setFrozenRows(1);

  const bodyRows = sheet.getMaxRows() - 1;
  if (dateColIdx) sheet.getRange(2, dateColIdx, bodyRows, 1).setNumberFormat(FMT_DATE);
  if (valueColIdxs) {
    for (const c of valueColIdxs) sheet.getRange(2, c, bodyRows, 1).setNumberFormat(FMT_18);
  }
  // Text format MUST be applied BEFORE any setValues on these cells, otherwise
  // a numeric-looking string is auto-coerced to a float and precision is lost.
  if (textColIdxs) {
    for (const c of textColIdxs) sheet.getRange(2, c, bodyRows, 1).setNumberFormat('@');
  }
  sheet.getRange(1, 1, 1, headers.length).createFilter();
  return sheet;
}

// Creates (or resets) a derived sheet with a bold, frozen header row.
function prepareDerivedSheet_(ss, name, headers) {
  let sheet = ss.getSheetByName(name);
  if (!sheet) sheet = ss.insertSheet(name);
  sheet.clearContents();
  sheet.appendRow(headers);
  sheet.getRange(1, 1, 1, headers.length).setFontWeight('bold');
  sheet.setFrozenRows(1);
  return sheet;
}

// Reads cursor (lastBlock, lastLogIdx) from the last data row of a raw sheet.
// Returns { lb: 0, li: -1 } if the sheet has no data rows.
function sheetCursor_(sheet) {
  if (sheet.getLastRow() < 2) return { lb: 0, li: -1 };
  const vals = sheet.getRange(sheet.getLastRow(), 2, 1, 2).getValues()[0];
  return { lb: Number(vals[0]), li: Number(vals[1]) };
}

// Appends rows to a raw sheet and applies date/value/text number formats.
// Text format is applied BEFORE setValues so BigInt strings aren't coerced to floats.
function appendRawRows_(sheet, rows, dateColIdx, valueColIdxs, textColIdxs) {
  if (!rows.length) return;
  const startRow = sheet.getLastRow() + 1;

  if (textColIdxs) {
    for (const c of textColIdxs) sheet.getRange(startRow, c, rows.length, 1).setNumberFormat('@');
  }

  sheet.getRange(startRow, 1, rows.length, rows[0].length).setValues(rows);

  if (dateColIdx) sheet.getRange(startRow, dateColIdx, rows.length, 1).setNumberFormat(FMT_DATE);
  if (valueColIdxs) {
    for (const c of valueColIdxs) sheet.getRange(startRow, c, rows.length, 1).setNumberFormat(FMT_18);
  }
}

// Removes any existing filter and creates a new one covering the entire data range.
function applyFilter_(sheet) {
  const existing = sheet.getFilter();
  if (existing) existing.remove();
  if (sheet.getLastRow() > 0) sheet.getDataRange().createFilter();
}

// -----------------------------------------------------------------------------
// readTransferRowsWithWei_ — reads RS_evt_Transfer with exact-integer wei values.
// Returns [{ txHash, bn, li, ts, from, to, value (float, display), valueWei (BigInt) }, …].
// -----------------------------------------------------------------------------
function readTransferRowsWithWei_(txSheet) {
  if (!txSheet || txSheet.getLastRow() < 2) return [];
  const numRows = txSheet.getLastRow() - 1;
  const rows    = txSheet.getRange(2, 1, numRows, RAW_TRANSFER_HEADERS.length).getValues();

  return rows.map((r, rowIdx) => {
    // Text format '@' + string setValue preserves the string exactly. If the format
    // was ever lost, Sheets may return a Number — that's precision loss.
    const weiStr = String(r[7]).trim();
    if (!/^-?\d+$/.test(weiStr)) {
      throw new Error('Row ' + (rowIdx + 2) + ' of ' + SHEET_RS_EVT_TRANSFER +
        ' has invalid value_wei="' + weiStr + '". Delete the sheet and re-run the import.');
    }
    return {
      txHash:   r[0],
      bn:       Number(r[1]),
      li:       Number(r[2]),
      ts:       r[3],
      from:     normalizeAddr_(r[4]),
      to:       normalizeAddr_(r[5]),
      value:    Number(r[6]),
      valueWei: BigInt(weiStr),
    };
  });
}

// =============================================================================
// Ratio lookup helpers  (used by rebuildAllWallets / rebuildWalletsSummary)
// =============================================================================

// Loads (block_num, ratio) pairs from a derived sheet, filtered to rows where
// ratio is non-empty. Returns a sorted-ascending array of [blockNum, ratio].
// blockCol and ratioCol are 1-indexed column numbers.
function loadRatioLookup_(sheetName, blockCol, ratioCol) {
  const ss    = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(sheetName);
  if (!sheet || sheet.getLastRow() < 2) return [];
  const numRows = sheet.getLastRow() - 1;
  const maxCol  = Math.max(blockCol, ratioCol);
  const data    = sheet.getRange(2, 1, numRows, maxCol).getValues();
  return data
    .filter(r => {
      const v = r[ratioCol - 1];
      return v !== '' && v !== null && v !== 0;
    })
    .map(r => [Number(r[blockCol - 1]), Number(r[ratioCol - 1])])
    .sort((a, b) => a[0] - b[0]);
}

// Binary search (O(log n)) replacement for the old linear lookupApprox_.
// Returns the ratio for the largest block_num ≤ bn, or '' if none.
function binarySearchApprox_(lookup, bn) {
  if (lookup.length === 0 || lookup[0][0] > bn) return '';
  let lo = 0, hi = lookup.length - 1;
  while (lo < hi) {
    const mid = (lo + hi + 1) >>> 1;
    if (lookup[mid][0] <= bn) lo = mid;
    else                      hi = mid - 1;
  }
  return lookup[lo][1];
}

// =============================================================================
// Transfer classification
// =============================================================================

// Derives the wallet-facing event type + counterparty + delta for one side of a
// transfer. `side` is 'from' or 'to'; the caller is responsible for skipping
// non-wallet addresses (zero, silo, fee receivers) before calling.
function classifySide_(tr, side) {
  const isMint = tr.from === ZERO_ADDR;
  const isBurn = tr.to === ZERO_ADDR || tr.to === SILO_CONTRACT;
  if (side === 'from') {
    return {
      eventType:    isBurn ? 'Burn' : 'TransferOut',
      counterparty: tr.to,
      delta:        -tr.value,
      deltaWei:     -tr.valueWei,
    };
  }
  return {
    eventType:    isMint ? 'Mint' : 'TransferIn',
    counterparty: tr.from,
    delta:        tr.value,
    deltaWei:     tr.valueWei,
  };
}

// Single-wallet variant: classify a transfer as seen by a specific wallet.
function classifyForWallet_(tr, wallet) {
  if (tr.from === ZERO_ADDR) {
    return { eventType: 'Mint', counterparty: ZERO_ADDR,
             delta: tr.value, deltaWei: tr.valueWei };
  }
  if (tr.to === ZERO_ADDR || tr.to === SILO_CONTRACT) {
    return { eventType: 'Burn', counterparty: tr.to,
             delta: -tr.value, deltaWei: -tr.valueWei };
  }
  if (tr.to === wallet) {
    return { eventType: 'TransferIn', counterparty: tr.from,
             delta: tr.value, deltaWei: tr.valueWei };
  }
  return { eventType: 'TransferOut', counterparty: tr.to,
           delta: -tr.value, deltaWei: -tr.valueWei };
}

// =============================================================================
// Phase 1 — Import functions  (API calls → raw event sheets only)
// =============================================================================

// -----------------------------------------------------------------------------
// importBalancesUpdated  →  RP_evt_BalancesUpdated
// -----------------------------------------------------------------------------
// Fetches BalancesUpdated from all four rocketNetworkBalances contracts.
// Stores: tx_hash, block_num, log_idx, block_timestamp, version,
//         total_eth, staking_eth, reth_supply.
// Both v1/v2 and v3/v4 ABI layouts place totalEth, stakingEth, rethSupply at
// data-word indices 1, 2, 3 — the decoding is identical.
// -----------------------------------------------------------------------------
function importBalancesUpdated(silent) {
  silent = (silent === true);
  let API_KEY;
  try { API_KEY = getApiKey_(); }
  catch (e) { Logger.log('importBalancesUpdated: ' + e.message); if (!silent) tryAlert_(e.message); return null; }

  const sheet  = ensureRawSheet_(SHEET_RP_EVT_BALANCES, RAW_BALANCES_HEADERS, 4, [6, 7, 8]);
  const cursor = sheetCursor_(sheet);

  Logger.log(SHEET_RP_EVT_BALANCES + ': cursor block=' + cursor.lb + ' log_idx=' + cursor.li);

  const newRows      = [];
  let   pageLimitHit = false;

  for (const contract of RNB_CONTRACTS) {
    let result;
    try {
      result = etherscanGetLogs_(contract.address, contract.topic, cursor.lb, API_KEY);
    } catch (e) {
      Logger.log('importBalancesUpdated error (' + contract.version + '): ' + e.message);
      if (!silent) tryAlert_('Etherscan error (' + contract.version + '): ' + e.message);
      return null;
    }
    if (result.truncated) pageLimitHit = true;

    for (const log of result.logs) {
      const bn = hexToInt_(log.blockNumber);
      const li = hexToInt_(log.logIndex);
      if (bn < cursor.lb || (bn === cursor.lb && li <= cursor.li)) continue;

      const w = dataWords_(log.data, 5);
      const totalEth   = wordToEth_(w[1]);
      const stakingEth = wordToEth_(w[2]);
      const rethSupply = wordToEth_(w[3]);
      if (rethSupply <= 0) continue;

      newRows.push([
        log.transactionHash,
        bn,
        li,
        new Date(hexToInt_(log.timeStamp) * 1000),
        contract.version,
        totalEth,
        stakingEth,
        rethSupply,
      ]);
    }
  }

  if (newRows.length === 0) {
    Logger.log(SHEET_RP_EVT_BALANCES + ': no new events.');
    if (!silent) tryAlert_('No new BalancesUpdated events after block ' + cursor.lb + '.');
    return 'No new data';
  }

  newRows.sort((a, b) => a[1] - b[1] || a[2] - b[2]);
  appendRawRows_(sheet, newRows, 4, [6, 7, 8]);
  Logger.log(SHEET_RP_EVT_BALANCES + ': appended ' + newRows.length + ' rows.');

  if (pageLimitHit) {
    if (!silent) tryAlert_(newRows.length + ' BalancesUpdated rows fetched (page limit). Run again for more.');
    return newRows.length + ' new rows (page limit reached, run again)';
  }
  return newRows.length + ' new rows';
}

// -----------------------------------------------------------------------------
// importRockBaseEvents  →  RS_evt_TotalAssetsUpdated + RS_evt_SettleRedeem
// -----------------------------------------------------------------------------
function importRockBaseEvents(silent) {
  silent = (silent === true);
  let API_KEY;
  try { API_KEY = getApiKey_(); }
  catch (e) { Logger.log('importRockBaseEvents: ' + e.message); if (!silent) tryAlert_(e.message); return null; }

  const taSheet = ensureRawSheet_(SHEET_RS_EVT_TA, RAW_TA_HEADERS, 4, [5]);
  const srSheet = ensureRawSheet_(SHEET_RS_EVT_SR, RAW_SR_HEADERS, 4, [7, 8, 9, 10]);

  const taCursor = sheetCursor_(taSheet);
  const srCursor = sheetCursor_(srSheet);

  // Use the minimum of the two cursors to avoid missing any events when the
  // two sheets are at different blocks (e.g. after a partial run).
  const fromBlock = Math.min(taCursor.lb, srCursor.lb);

  Logger.log('RS_evt base: fromBlock=' + fromBlock);

  let taResult, srResult;
  try {
    taResult = etherscanGetLogs_(ROCK_RETH_PROXY, TOPICS.TOTAL_ASSETS_UPDATED, fromBlock, API_KEY);
    srResult = etherscanGetLogs_(ROCK_RETH_PROXY, TOPICS.SETTLE_REDEEM,        fromBlock, API_KEY);
  } catch (e) {
    Logger.log('importRockBaseEvents error: ' + e.message);
    if (!silent) tryAlert_('Etherscan error (RockSolid base events): ' + e.message);
    return null;
  }

  const pageLimitHit = taResult.truncated || srResult.truncated;

  // TotalAssetsUpdated: data = [totalAssets]
  const taRows = taResult.logs
    .filter(log => {
      const bn = hexToInt_(log.blockNumber), li = hexToInt_(log.logIndex);
      return bn > taCursor.lb || (bn === taCursor.lb && li > taCursor.li);
    })
    .map(log => [
      log.transactionHash,
      hexToInt_(log.blockNumber),
      hexToInt_(log.logIndex),
      new Date(hexToInt_(log.timeStamp) * 1000),
      wordToEth_(dataWords_(log.data, 1)[0]),
    ]);

  // SettleRedeem: indexed topics[1]=epochId, topics[2]=settledId;
  //   data = [totalAssets, totalSupply, assetsWithdrawed, sharesBurned]
  const srRows = srResult.logs
    .filter(log => {
      const bn = hexToInt_(log.blockNumber), li = hexToInt_(log.logIndex);
      return bn > srCursor.lb || (bn === srCursor.lb && li > srCursor.li);
    })
    .map(log => {
      const w = dataWords_(log.data, 4);
      return [
        log.transactionHash,
        hexToInt_(log.blockNumber),
        hexToInt_(log.logIndex),
        new Date(hexToInt_(log.timeStamp) * 1000),
        hexToInt_(log.topics[1]),
        hexToInt_(log.topics[2]),
        wordToEth_(w[0]),
        wordToEth_(w[1]),
        wordToEth_(w[2]),
        wordToEth_(w[3]),
      ];
    });

  taRows.sort((a, b) => a[1] - b[1] || a[2] - b[2]);
  srRows.sort((a, b) => a[1] - b[1] || a[2] - b[2]);

  appendRawRows_(taSheet, taRows, 4, [5]);
  appendRawRows_(srSheet, srRows, 4, [7, 8, 9, 10]);

  const total = taRows.length + srRows.length;
  Logger.log('RS_evt base: appended ' + taRows.length + ' TotalAssetsUpdated + ' +
             srRows.length + ' SettleRedeem rows.');

  if (pageLimitHit) {
    if (!silent) tryAlert_(total + ' RockSolid base rows fetched (page limit). Run again for more.');
    return total + ' new rows (page limit reached, run again)';
  }
  if (!silent) tryAlert_(total + ' RockSolid base rows imported.');
  return total + ' new rows';
}

// -----------------------------------------------------------------------------
// importTransfers  →  RS_evt_Transfer
// -----------------------------------------------------------------------------
// Fetches ALL Transfer events from the rock.rETH token contract in one query —
// mints (from=0x0), burns (to=0x0), silo redemptions, and peer-to-peer transfers.
// -----------------------------------------------------------------------------
function importTransfers(silent) {
  silent = (silent === true);
  let API_KEY;
  try { API_KEY = getApiKey_(); }
  catch (e) { Logger.log('importTransfers: ' + e.message); if (!silent) tryAlert_(e.message); return null; }

  const sheet = ensureRawSheet_(SHEET_RS_EVT_TRANSFER, RAW_TRANSFER_HEADERS, 4, [7], [8]);
  const { lb, li } = sheetCursor_(sheet);

  Logger.log(SHEET_RS_EVT_TRANSFER + ': cursor block=' + lb + ' li=' + li);

  let result;
  try {
    result = etherscanGetLogs_(ROCK_RETH_PROXY, TOPICS.TRANSFER, lb, API_KEY);
  } catch (e) {
    Logger.log('importTransfers error: ' + e.message);
    if (!silent) tryAlert_('Etherscan error (transfers): ' + e.message);
    return null;
  }

  const pageLimitHit = result.truncated;

  const newLogs = result.logs
    .filter(log => {
      const bn = hexToInt_(log.blockNumber), li2 = hexToInt_(log.logIndex);
      return bn > lb || (bn === lb && li2 > li);
    })
    .sort((a, b) => {
      const d = hexToInt_(a.blockNumber) - hexToInt_(b.blockNumber);
      return d !== 0 ? d : hexToInt_(a.logIndex) - hexToInt_(b.logIndex);
    });

  const newRows = newLogs.map(log => {
    const word     = dataWords_(log.data, 1)[0];
    const valueWei = BigInt('0x' + word);
    const valueEth = Number(valueWei) / WEI_PER_ETH_NUM;
    return [
      log.transactionHash,
      hexToInt_(log.blockNumber),
      hexToInt_(log.logIndex),
      new Date(hexToInt_(log.timeStamp) * 1000),
      topicToAddr_(log.topics[1]),
      topicToAddr_(log.topics[2]),
      valueEth,
      valueWei.toString(),
    ];
  });

  appendRawRows_(sheet, newRows, 4, [7], [8]);
  Logger.log(SHEET_RS_EVT_TRANSFER + ': appended ' + newRows.length + ' new rows.');

  if (pageLimitHit) {
    if (!silent) tryAlert_(newRows.length + ' Transfer rows fetched (page limit). Run again for more.');
    return newRows.length + ' new rows (page limit reached, run again)';
  }
  if (!silent) tryAlert_(newRows.length + ' Transfer rows imported.');
  return newRows.length + ' new rows';
}

// -----------------------------------------------------------------------------
// importRockSolidVaultEvents — base events + transfers in one operation.
// -----------------------------------------------------------------------------
function importRockSolidVaultEvents() {
  const lines = [];
  const push  = (label, r) => { if (r) lines.push(label + ': ' + r); };

  push('RockSolid base events', importRockBaseEvents(true));
  push('Transfers',             importTransfers(true));

  tryAlert_(lines.length ? lines.join('\n') : 'No new RockSolid Vault events found.');
}

// -----------------------------------------------------------------------------
// importAll — runs all import phases then rebuilds all derived sheets
// -----------------------------------------------------------------------------
function importAll() {
  const lines = [];
  const push  = (label, r) => { if (r) lines.push(label + ': ' + r); };

  push('BalancesUpdated',       importBalancesUpdated(true));
  push('RockSolid base events', importRockBaseEvents(true));
  push('Transfers',             importTransfers(true));

  rebuildAll(true);

  if (lines.length) tryAlert_(lines.join('\n'));
}

// =============================================================================
// Phase 2 — Rebuild functions  (no API calls, raw event sheets → derived sheets)
// =============================================================================

// ── Derived sheet column layouts ─────────────────────────────────────────────
//
//   rETH_ratio:   A tx_hash | B block_num | C log_idx | D blockTimestamp
//                 E event_type | F totalEth | G stakingEth | H rethSupply
//                 I ratio | J version | K apy_7d | L apy_30d
//
//   rock.rETH_ratio: A tx_hash | B block_num | C log_idx | D blockTimestamp
//                    E event_type | F totalAssets | G totalSupply | H ratio
//                    I from | J to | K rock_shares | L reth_amt
//                    M rETH/ETH | N rock.rETH/ETH
//                    O apy_7d_rETH | P apy_30d_rETH | Q apy_7d_ETH | R apy_30d_ETH
//
//   wallets_all:  A wallet | B tx_hash | C block_num | D log_idx | E blockTimestamp
//                 F event_type | G counterparty | H rock.rETH_delta
//                 I rock_rETH_ratio | J rETH_ratio | K rETH_delta | L ETH_delta
//                 M rock.rETH_total | N rETH_value | O ETH_value | P ETH_gain
//
//   wallets_summary: A wallet | B last_block | C last_timestamp
//                    D rock.rETH_total | E rock_rETH_ratio | F rETH_ratio
//                    G rETH_value | H ETH_value | I total_ETH_delta | J ETH_gain
//
//   wallet_<addr>: A tx_hash | B block_num | C log_idx | D blockTimestamp
//                  E event_type | F counterparty | G rock.rETH_delta
//                  H rock_rETH_ratio | I rETH_ratio | J rETH_delta | K ETH_delta
//                  L rock.rETH_total | M rETH_value | N ETH_value | O ETH_gain

// -----------------------------------------------------------------------------
// rebuildRethRatio — reads RP_evt_BalancesUpdated, writes rETH_ratio
// -----------------------------------------------------------------------------
function rebuildRethRatio() {
  const ss       = SpreadsheetApp.getActiveSpreadsheet();
  const rawSheet = ss.getSheetByName(SHEET_RP_EVT_BALANCES);
  if (!rawSheet || rawSheet.getLastRow() < 2) {
    tryAlert_(SHEET_RP_EVT_BALANCES + ' is empty. Run "Import: Rocket Pool events" first.');
    return;
  }

  const HEADERS = [
    'tx_hash', 'block_num', 'log_idx', 'blockTimestamp', 'event_type',
    'totalEth_ETH', 'stakingEth_ETH', 'rethSupply_ETH', 'ratio', 'version',
    'apy_7d', 'apy_30d',
  ];
  const sheet = prepareDerivedSheet_(ss, SHEET_RETH_RATIO, HEADERS);

  // Raw cols: tx_hash[0] block_num[1] log_idx[2] block_ts[3] version[4]
  //           total_eth[5] staking_eth[6] reth_supply[7]
  const numRaw = rawSheet.getLastRow() - 1;
  const raw    = rawSheet.getRange(2, 1, numRaw, 8).getValues();
  raw.sort((a, b) => a[1] - b[1] || a[2] - b[2]);

  const rows = raw.map(r => [
    r[0],              // A tx_hash
    r[1],              // B block_num
    r[2],              // C log_idx
    r[3],              // D blockTimestamp
    'BalancesUpdated', // E event_type
    r[5],              // F totalEth
    r[6],              // G stakingEth
    r[7],              // H rethSupply
    '',                // I ratio — filled below
    r[4],              // J version
  ]);

  // Ratio: totalEth / rethSupply, blanked for all events except the last per block.
  for (let i = 0; i < rows.length; i++) {
    const isLast = i === rows.length - 1 || rows[i][1] !== rows[i + 1][1];
    const te = Number(raw[i][5]), rs = Number(raw[i][7]);
    rows[i][8] = isLast && rs > 0 ? te / rs : '';
  }

  const firstDataRow = 2;
  sheet.getRange(firstDataRow, 1, rows.length, rows[0].length).setValues(rows);
  sheet.getRange(firstDataRow, 4, rows.length, 1).setNumberFormat(FMT_DATE);
  sheet.getRange(firstDataRow, 6, rows.length, 3).setNumberFormat(FMT_18); // F, G, H
  sheet.getRange(firstDataRow, 9, rows.length, 1).setNumberFormat(FMT_18); // I ratio

  // APY formulas (cols K, L)
  const apyFormulas = rows.map((_, i) => {
    const r = firstDataRow + i;
    return [
      '=IFERROR((I' + r + '/VLOOKUP(D' + r + '-7,D$2:I,6,TRUE))^(365/7)-1,"")',
      '=IFERROR((I' + r + '/VLOOKUP(D' + r + '-30,D$2:I,6,TRUE))^(365/30)-1,"")',
    ];
  });
  sheet.getRange(firstDataRow, 11, apyFormulas.length, 2).setFormulas(apyFormulas);
  sheet.getRange(firstDataRow, 11, apyFormulas.length, 2).setNumberFormat('0.00%');

  applyFilter_(sheet);
  Logger.log('rETH_ratio: rebuilt from ' + rows.length + ' raw rows.');
}

// -----------------------------------------------------------------------------
// rebuildRockRatio — reads RS_evt_TotalAssetsUpdated + RS_evt_SettleRedeem +
//                    RS_evt_Transfer (mints/burns) + rETH_ratio (BalancesUpdated),
//                    merges all events chronologically, forward-fills both ratio
//                    columns, then writes rock.rETH_ratio.
//
// Column layout:
//   A–L  unchanged (tx_hash … rETH)
//   M    rETH/ETH   — actual value on RP rows, carry-forward on rock rows
//   N    rock.rETH/ETH = H × M for every row
//   O–R  APY formulas
// -----------------------------------------------------------------------------
function rebuildRockRatio() {
  const ss      = SpreadsheetApp.getActiveSpreadsheet();
  const taSheet = ss.getSheetByName(SHEET_RS_EVT_TA);
  const srSheet = ss.getSheetByName(SHEET_RS_EVT_SR);
  const txSheet = ss.getSheetByName(SHEET_RS_EVT_TRANSFER);

  if (!txSheet || txSheet.getLastRow() < 2) {
    tryAlert_(SHEET_RS_EVT_TRANSFER + ' is empty. Run "Import: RockSolid Vault events" first.');
    return;
  }

  // TotalAssetsUpdated: tx[0] bn[1] li[2] ts[3] totalAssets[4]
  const taRows = (taSheet && taSheet.getLastRow() > 1)
    ? taSheet.getRange(2, 1, taSheet.getLastRow() - 1, 5).getValues().map(r => ({
        src: 'TotalAssetsUpdated', txHash: r[0], bn: Number(r[1]), li: Number(r[2]),
        ts: r[3], totalAssets: Number(r[4]),
      }))
    : [];

  // SettleRedeem: tx[0] bn[1] li[2] ts[3] epoch[4] settled[5]
  //   totalAssets[6] totalSupply[7] assetsWithdrawed[8] sharesBurned[9]
  const srRows = (srSheet && srSheet.getLastRow() > 1)
    ? srSheet.getRange(2, 1, srSheet.getLastRow() - 1, 10).getValues().map(r => ({
        src: 'SettleRedeem', txHash: r[0], bn: Number(r[1]), li: Number(r[2]), ts: r[3],
        totalAssets: Number(r[6]), totalSupply: Number(r[7]),
        assetsWithdrawed: Number(r[8]), sharesBurned: Number(r[9]),
      }))
    : [];

  // Transfer rows with exact wei (we need BigInt for supply accounting below).
  const txRecords = readTransferRowsWithWei_(txSheet);
  const mintBurnRows = txRecords
    .filter(tr => tr.from === ZERO_ADDR || tr.to === ZERO_ADDR)
    .map(tr => ({
      src:      tr.from === ZERO_ADDR ? 'Transfer_Mint' : 'Transfer_Burn',
      txHash:   tr.txHash,
      bn:       tr.bn,
      li:       tr.li,
      ts:       tr.ts,
      from:     tr.from,
      to:       tr.to,
      value:    tr.value,
      valueWei: tr.valueWei,
    }));

  // RP BalancesUpdated events: load from the already-rebuilt rETH_ratio derived sheet.
  // rebuildRethRatio() always runs before rebuildRockRatio() in rebuildAll(), so this
  // sheet is up to date. We only take rows with a non-empty ratio (col I, index 8) —
  // those are the per-block canonical values already deduplicated by rebuildRethRatio.
  const rethRatioSheet = ss.getSheetByName(SHEET_RETH_RATIO);
  const rpRows = (rethRatioSheet && rethRatioSheet.getLastRow() > 1)
    ? rethRatioSheet
        .getRange(2, 1, rethRatioSheet.getLastRow() - 1, 9)
        .getValues()
        .filter(r => r[8] !== '' && r[8] !== null && r[8] !== 0)
        .map(r => ({
          src: 'BalancesUpdated', txHash: r[0],
          bn: Number(r[1]), li: Number(r[2]), ts: r[3],
          rethRatio: Number(r[8]),
        }))
    : [];

  const allEvents = [...taRows, ...srRows, ...mintBurnRows, ...rpRows]
    .sort((a, b) => a.bn - b.bn || a.li - b.li);

  if (allEvents.length === 0) {
    tryAlert_('No rock.rETH events found in raw sheets.');
    return;
  }

  // Supply tracked as BigInt (exact integer wei); assets tracked as float because
  // the ratio-preserving rescale on mint requires multiplication anyway.
  let supplyWei       = BIG_ZERO;
  let lastTotalAssets = 0;

  const sheetRows = allEvents.map(ev => {
    // RP BalancesUpdated row: H (rock ratio) will be filled in the forward-fill pass.
    // M holds the actual rETH/ETH; N (rock.rETH/ETH) is filled in the same pass.
    if (ev.src === 'BalancesUpdated') {
      return [
        ev.txHash, ev.bn, ev.li, ev.ts, 'BalancesUpdated',
        '', '', '',       // F totalAssets, G supply, H rock.rETH/rETH (filled later)
        '', '', '', '',   // I from, J to, K rockShares, L rethAmt
        ev.rethRatio,     // M actual rETH/ETH
        '',               // N rock.rETH/ETH (filled later)
      ];
    }

    let eventType = ev.src;
    let rockShares = '', rethAmt = '', fromAddr = '', toAddr = '';

    if (ev.src === 'SettleRedeem') {
      lastTotalAssets = ev.totalAssets;
      supplyWei       = BigInt(Math.round(ev.totalSupply * WEI_PER_ETH_NUM));
      rethAmt    = ev.assetsWithdrawed;
      rockShares = ev.sharesBurned;

    } else if (ev.src === 'TotalAssetsUpdated') {
      lastTotalAssets = ev.totalAssets;

    } else if (ev.src === 'Transfer_Mint') {
      fromAddr = ev.from;
      toAddr   = ev.to;
      const vNum = ev.value;
      if (FEE_RECEIVER_ADDRESSES.has(toAddr)) {
        eventType = 'Mint (fees)';
      } else {
        eventType = 'Mint';
        // Preserve ratio: scale assets up by (supply+mint)/supply.
        const supplyFloat = Number(supplyWei) / WEI_PER_ETH_NUM;
        if (supplyFloat > 0) {
          lastTotalAssets = lastTotalAssets * (supplyFloat + vNum) / supplyFloat;
        }
      }
      supplyWei += ev.valueWei;
      rockShares = vNum;

    } else { // Transfer_Burn
      fromAddr = ev.from;
      toAddr   = ev.to;
      const vNum = ev.value;
      eventType  = 'Burn';
      const supplyFloat = Number(supplyWei) / WEI_PER_ETH_NUM;
      if (supplyFloat > 0) {
        lastTotalAssets = lastTotalAssets * (supplyFloat - vNum) / supplyFloat;
      }
      supplyWei -= ev.valueWei;
      rockShares = vNum;
    }

    const supplyFloat = Number(supplyWei) / WEI_PER_ETH_NUM;
    const ratio       = supplyFloat > 0 ? lastTotalAssets / supplyFloat : '';
    const isTransfer  = ev.src === 'Transfer_Mint' || ev.src === 'Transfer_Burn';
    if (isTransfer && ratio !== '') rethAmt = rockShares * ratio;

    return [
      ev.txHash,       // A
      ev.bn,           // B
      ev.li,           // C
      ev.ts,           // D
      eventType,       // E
      lastTotalAssets, // F totalAssets
      supplyFloat,     // G totalSupply
      ratio,           // H (may be blanked by same-block dedup below)
      fromAddr,        // I
      toAddr,          // J
      rockShares,      // K
      rethAmt,         // L
      '',              // M rETH/ETH (filled in forward-fill pass)
      '',              // N rock.rETH/ETH (filled in forward-fill pass)
    ];
  });

  // Same-block dedup for rock events: keep H only for the last rock event per block.
  // RP (BalancesUpdated) rows are excluded — they start with H='' and are handled
  // separately in the forward-fill pass, so a RP row following a rock row in the
  // same block must NOT cause the rock row's H to be blanked.
  const lastRockIdxPerBlock = new Map();
  for (let i = 0; i < sheetRows.length; i++) {
    if (sheetRows[i][4] !== 'BalancesUpdated') lastRockIdxPerBlock.set(sheetRows[i][1], i);
  }
  for (let i = 0; i < sheetRows.length; i++) {
    if (sheetRows[i][4] !== 'BalancesUpdated' && lastRockIdxPerBlock.get(sheetRows[i][1]) !== i) {
      sheetRows[i][7] = '';
    }
  }

  // Forward-fill pass: propagate the latest known value of each ratio to rows that
  // don't have their own, then compute N = H × M for every row.
  //
  //   RP event row  → H = carry-forward from last rock event; M = own actual rETH/ETH
  //   Rock event row → M = carry-forward from last RP event;  H = own computed ratio (or '')
  //
  // Update order: read the row's own value first, then carry-forward.
  let lastH = '', lastM = '';
  for (const row of sheetRows) {
    if (row[4] === 'BalancesUpdated') {   // RP event
      row[7]  = lastH;                    // H = carry-forward rock ratio
      lastM   = row[12];                  // update state from actual rETH/ETH
    } else {                              // rock event
      row[12] = lastM;                    // M = carry-forward rETH/ETH
      if (row[7] !== '') lastH = row[7];  // update state if H was not blanked by dedup
    }
    row[13] = (row[7] !== '' && row[12] !== '') ? row[7] * row[12] : '';
  }

  const HEADERS = [
    'tx_hash', 'block_num', 'log_idx', 'blockTimestamp', 'event_type',
    'totalAssets_rETH', 'totalSupply_rETH', 'rock.rETH/rETH',
    'from', 'to', 'rock_rETH', 'rETH',
    'rETH/ETH', 'rock.rETH/ETH',
    'apy_7d_rETH', 'apy_30d_rETH', 'apy_7d_ETH', 'apy_30d_ETH',
  ];
  const sheet = prepareDerivedSheet_(ss, SHEET_ROCK_RATIO, HEADERS);

  const firstDataRow = 2;
  sheet.getRange(firstDataRow, 1, sheetRows.length, sheetRows[0].length).setValues(sheetRows);
  sheet.getRange(firstDataRow, 4, sheetRows.length, 1).setNumberFormat(FMT_DATE);
  sheet.getRange(firstDataRow, 6, sheetRows.length, 3).setNumberFormat(FMT_18);  // F, G, H
  sheet.getRange(firstDataRow, 11, sheetRows.length, 2).setNumberFormat(FMT_18); // K, L
  sheet.getRange(firstDataRow, 13, sheetRows.length, 2).setNumberFormat(FMT_18); // M (rETH/ETH), N (rock.rETH/ETH)

  // Formula columns O–R (APY).
  // D$2:H spans 5 cols → offset 5 = col H (rock.rETH/rETH).
  // D$2:N spans 11 cols → offset 11 = col N (rock.rETH/ETH).
  const rockFormulas = sheetRows.map((_, i) => {
    const r = firstDataRow + i;
    return [
      '=IF(H' + r + '="","",IFERROR((H' + r + '/VLOOKUP(D' + r + '-7,D$2:H,5,TRUE))^(365/7)-1,""))',
      '=IF(H' + r + '="","",IFERROR((H' + r + '/VLOOKUP(D' + r + '-30,D$2:H,5,TRUE))^(365/30)-1,""))',
      '=IF(N' + r + '="","",IFERROR((N' + r + '/VLOOKUP(D' + r + '-7,D$2:N,11,TRUE))^(365/7)-1,""))',
      '=IF(N' + r + '="","",IFERROR((N' + r + '/VLOOKUP(D' + r + '-30,D$2:N,11,TRUE))^(365/30)-1,""))',
    ];
  });
  sheet.getRange(firstDataRow, 15, rockFormulas.length, 4).setFormulas(rockFormulas);
  sheet.getRange(firstDataRow, 15, rockFormulas.length, 4).setNumberFormat('0.00%');

  applyFilter_(sheet);
  Logger.log('rock.rETH_ratio: rebuilt from ' + sheetRows.length + ' rows (' +
             rpRows.length + ' BalancesUpdated + ' +
             (sheetRows.length - rpRows.length) + ' rock events).');
}

// -----------------------------------------------------------------------------
// rebuildAllWallets — reads RS_evt_Transfer (all non-fee mints + burns),
//                     writes wallets_all with per-wallet position tracking
// -----------------------------------------------------------------------------
function rebuildAllWallets() {
  const ss      = SpreadsheetApp.getActiveSpreadsheet();
  const txSheet = ss.getSheetByName(SHEET_RS_EVT_TRANSFER);

  if (!txSheet || txSheet.getLastRow() < 2) {
    tryAlert_(SHEET_RS_EVT_TRANSFER + ' is empty. Run "Import: RockSolid Vault events" first.');
    return;
  }

  // rock.rETH_ratio col B = block_num (2), col H = rock.rETH/rETH ratio (8)
  // rETH_ratio      col B = block_num (2), col I = rETH/ETH ratio (9)
  const rockRatioLookup = loadRatioLookup_(SHEET_ROCK_RATIO, 2, 8);
  const rethRatioLookup = loadRatioLookup_(SHEET_RETH_RATIO, 2, 9);

  if (rockRatioLookup.length === 0 || rethRatioLookup.length === 0) {
    tryAlert_('Ratio sheets are empty. Run "Rebuild: rETH ratio" and "Rebuild: rock.rETH ratio" first.');
    return;
  }

  let txRecords;
  try { txRecords = readTransferRowsWithWei_(txSheet); }
  catch (e) { tryAlert_(e.message); return; }

  // Addresses that are NOT real user wallets — excluded from wallet identity.
  const NON_WALLET = new Set([ZERO_ADDR, SILO_CONTRACT, ...FEE_RECEIVER_ADDRESSES]);

  // Expand each Transfer into 0, 1, or 2 wallet-events (one per real user wallet).
  const rawEvents = [];
  for (const tr of txRecords) {
    if (!NON_WALLET.has(tr.from)) {
      const cls = classifySide_(tr, 'from');
      rawEvents.push({ wallet: tr.from, txHash: tr.txHash, bn: tr.bn, li: tr.li, ts: tr.ts, ...cls });
    }
    if (!NON_WALLET.has(tr.to)) {
      const cls = classifySide_(tr, 'to');
      rawEvents.push({ wallet: tr.to, txHash: tr.txHash, bn: tr.bn, li: tr.li, ts: tr.ts, ...cls });
    }
  }

  // Sort chronologically; within the same transfer order by wallet for determinism.
  rawEvents.sort((a, b) => a.bn - b.bn || a.li - b.li ||
                           (a.wallet < b.wallet ? -1 : a.wallet > b.wallet ? 1 : 0));

  const walletRockWei     = new Map();
  const walletEthDeltaSum = new Map();

  const dataRows = rawEvents.map(ev => {
    const { wallet, txHash, bn, li, ts, eventType, counterparty, delta, deltaWei } = ev;

    const rockRethRatio = binarySearchApprox_(rockRatioLookup, bn);
    const rethRatio     = binarySearchApprox_(rethRatioLookup, bn);

    const rethDelta = (rockRethRatio !== '')                         ? delta * rockRethRatio     : '';
    const ethDelta  = (rethDelta !== '' && rethRatio !== '')         ? rethDelta * rethRatio     : '';

    // Exact BigInt running total so a fully-redeemed wallet shows exactly 0.
    const prevWei = walletRockWei.get(wallet) || BIG_ZERO;
    const newWei  = prevWei + deltaWei;
    walletRockWei.set(wallet, newWei);
    const newTotal = Number(newWei) / WEI_PER_ETH_NUM;

    // ETH_delta accumulation is inherently float (ratios are floats).
    const prevEthDelta = walletEthDeltaSum.get(wallet) || 0;
    const newEthDelta  = prevEthDelta + (ethDelta !== '' ? Number(ethDelta) : 0);
    walletEthDeltaSum.set(wallet, newEthDelta);

    const rethValue = (rockRethRatio !== '')                    ? newTotal * rockRethRatio : '';
    const ethValue  = (rethValue !== '' && rethRatio !== '')    ? rethValue * rethRatio    : '';
    const ethGain   = (ethValue !== '')                         ? ethValue - newEthDelta   : '';

    return [
      wallet,                                    // A
      txHash,                                    // B
      bn,                                        // C
      li,                                        // D
      ts,                                        // E
      eventType,                                 // F
      counterparty,                              // G
      delta,                                     // H rock.rETH_delta
      rockRethRatio !== '' ? rockRethRatio : '', // I
      rethRatio     !== '' ? rethRatio     : '', // J
      rethDelta,                                 // K
      ethDelta,                                  // L
      newTotal,                                  // M (exact 0 when closed)
      rethValue,                                 // N
      ethValue,                                  // O
      ethGain,                                   // P
    ];
  });

  const HEADERS = [
    'wallet', 'tx_hash', 'block_num', 'log_idx', 'blockTimestamp', 'event_type', 'counterparty',
    'rock.rETH_delta',
    'rock_rETH_ratio', 'rETH_ratio', 'rETH_delta', 'ETH_delta',
    'rock.rETH_total', 'rETH_value', 'ETH_value', 'ETH_gain',
  ];
  const sheet = prepareDerivedSheet_(ss, SHEET_WALLETS_ALL, HEADERS);

  if (dataRows.length > 0) {
    const firstDataRow = 2;
    sheet.getRange(firstDataRow, 1, dataRows.length, HEADERS.length).setValues(dataRows);
    sheet.getRange(firstDataRow, 5,  dataRows.length, 1).setNumberFormat(FMT_DATE); // E
    sheet.getRange(firstDataRow, 8,  dataRows.length, 1).setNumberFormat(FMT_18);   // H
    sheet.getRange(firstDataRow, 9,  dataRows.length, 2).setNumberFormat(FMT_18);   // I, J
    sheet.getRange(firstDataRow, 11, dataRows.length, 6).setNumberFormat(FMT_18);   // K–P
  }

  applyFilter_(sheet);
  Logger.log('wallets_all: rebuilt ' + dataRows.length + ' wallet-events across ' +
             walletRockWei.size + ' wallets.');
}

// -----------------------------------------------------------------------------
// rebuildWalletsSummary — reads wallets_all + raw transfers, writes wallets_summary
// -----------------------------------------------------------------------------
function rebuildWalletsSummary() {
  const ss       = SpreadsheetApp.getActiveSpreadsheet();
  const allSheet = ss.getSheetByName(SHEET_WALLETS_ALL);
  const txSheet  = ss.getSheetByName(SHEET_RS_EVT_TRANSFER);

  if (!allSheet || allSheet.getLastRow() < 2) {
    tryAlert_(SHEET_WALLETS_ALL + ' is empty. Run "Rebuild: all wallets sheet" first.');
    return;
  }
  if (!txSheet || txSheet.getLastRow() < 2) {
    tryAlert_(SHEET_RS_EVT_TRANSFER + ' is empty. Run "Import: RockSolid Vault events" first.');
    return;
  }

  // Recompute rock.rETH totals EXACTLY from raw transfers using BigInt —
  // a fully-redeemed wallet must show exactly 0, not a float-drift value.
  let txRecords;
  try { txRecords = readTransferRowsWithWei_(txSheet); }
  catch (e) { tryAlert_(e.message); return; }

  const NON_WALLET = new Set([ZERO_ADDR, SILO_CONTRACT, ...FEE_RECEIVER_ADDRESSES]);
  const walletRockWei = new Map();
  for (const tr of txRecords) {
    if (!NON_WALLET.has(tr.from)) {
      walletRockWei.set(tr.from, (walletRockWei.get(tr.from) || BIG_ZERO) - tr.valueWei);
    }
    if (!NON_WALLET.has(tr.to)) {
      walletRockWei.set(tr.to,   (walletRockWei.get(tr.to)   || BIG_ZERO) + tr.valueWei);
    }
  }

  // wallets_all cols: wallet[0] tx_hash[1] block_num[2] log_idx[3] blockTimestamp[4]
  //   event_type[5] counterparty[6] rock.rETH_delta[7] rock_rETH_ratio[8] rETH_ratio[9]
  //   rETH_delta[10] ETH_delta[11] rock.rETH_total[12] rETH_value[13] ETH_value[14] ETH_gain[15]
  const numRows = allSheet.getLastRow() - 1;
  const rows    = allSheet.getRange(2, 1, numRows, 16).getValues();
  const walletLastRow  = new Map();
  const walletEthDelta = new Map();
  for (const r of rows) {
    const wallet = String(r[0]);
    walletLastRow.set(wallet, r);
    walletEthDelta.set(wallet, (walletEthDelta.get(wallet) || 0) + (r[11] !== '' ? Number(r[11]) : 0));
  }

  const rockRatioLookup = loadRatioLookup_(SHEET_ROCK_RATIO, 2, 8);
  const rethRatioLookup = loadRatioLookup_(SHEET_RETH_RATIO, 2, 9);
  const latestRockRatio = rockRatioLookup.length ? rockRatioLookup[rockRatioLookup.length - 1][1] : '';
  const latestRethRatio = rethRatioLookup.length ? rethRatioLookup[rethRatioLookup.length - 1][1] : '';

  const summaryRows = Array.from(walletLastRow.entries()).map(([wallet, r]) => {
    const rockTotalWei  = walletRockWei.get(wallet) || BIG_ZERO;
    const rockTotal     = Number(rockTotalWei) / WEI_PER_ETH_NUM;
    const totalEthDelta = walletEthDelta.get(wallet) || 0;
    const rethValue     = (latestRockRatio !== '')                    ? rockTotal * latestRockRatio : '';
    const ethValue      = (rethValue !== '' && latestRethRatio !== '') ? rethValue * latestRethRatio : '';
    const ethGain       = (ethValue !== '')                           ? ethValue - totalEthDelta    : '';

    return [
      wallet,           // A
      Number(r[2]),     // B last_block
      r[4],             // C last_timestamp
      rockTotal,        // D rock.rETH_total
      latestRockRatio,  // E
      latestRethRatio,  // F
      rethValue,        // G
      ethValue,         // H
      totalEthDelta,    // I
      ethGain,          // J
    ];
  });

  // Sort by ETH value descending, then wallet address.
  summaryRows.sort((a, b) => {
    const av = a[7] !== '' ? Number(a[7]) : -Infinity;
    const bv = b[7] !== '' ? Number(b[7]) : -Infinity;
    if (bv !== av) return bv - av;
    return a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0;
  });

  const HEADERS = [
    'wallet', 'last_block', 'last_timestamp',
    'rock.rETH_total', 'rock_rETH_ratio', 'rETH_ratio',
    'rETH_value', 'ETH_value', 'total_ETH_delta', 'ETH_gain',
  ];
  const sheet = prepareDerivedSheet_(ss, SHEET_WALLETS_SUMMARY, HEADERS);

  if (summaryRows.length > 0) {
    const firstDataRow = 2;
    sheet.getRange(firstDataRow, 1, summaryRows.length, HEADERS.length).setValues(summaryRows);
    sheet.getRange(firstDataRow, 3, summaryRows.length, 1).setNumberFormat(FMT_DATE);
    sheet.getRange(firstDataRow, 4, summaryRows.length, 7).setNumberFormat(FMT_18); // D–J
  }

  applyFilter_(sheet);
  Logger.log('wallets_summary: rebuilt with ' + summaryRows.length + ' wallets.');
}

// -----------------------------------------------------------------------------
// rebuildWallet — reads RS_evt_Transfer (filtered for wallet), writes wallet_<addr>
// -----------------------------------------------------------------------------
function rebuildWallet() {
  let wallet;
  try { wallet = getWalletAddress_(); }
  catch (e) { tryAlert_(e.message); return; }

  const ss      = SpreadsheetApp.getActiveSpreadsheet();
  const txSheet = ss.getSheetByName(SHEET_RS_EVT_TRANSFER);
  if (!txSheet || txSheet.getLastRow() < 2) {
    tryAlert_(SHEET_RS_EVT_TRANSFER + ' is empty. Run "Import: RockSolid Vault events" first.');
    return;
  }

  const HEADERS = [
    'tx_hash', 'block_num', 'log_idx', 'blockTimestamp', 'event_type', 'counterparty',
    'rock.rETH_delta',
    'rock_rETH_ratio', 'rETH_ratio', 'rETH_delta', 'ETH_delta',
    'rock.rETH_total', 'rETH_value', 'ETH_value', 'ETH_gain',
  ];
  const sheet = prepareDerivedSheet_(ss, 'wallet_' + wallet, HEADERS);

  let txRecords;
  try { txRecords = readTransferRowsWithWei_(txSheet); }
  catch (e) { tryAlert_(e.message); return; }

  const walletEvents = txRecords
    .filter(tr => tr.from === wallet || tr.to === wallet)
    .sort((a, b) => a.bn - b.bn || a.li - b.li);

  if (walletEvents.length === 0) {
    Logger.log('rebuildWallet: no Transfer events for wallet ' + wallet);
    appendSummaryRow_(sheet, 1);
    applyFilter_(sheet);
    return;
  }

  // Build per-row values. rock.rETH_total (col L) is pre-computed from a BigInt
  // running sum so a fully-closed position shows exactly 0.
  let runningWei = BIG_ZERO;
  const dataRows = walletEvents.map(tr => {
    const { eventType, counterparty, delta, deltaWei } = classifyForWallet_(tr, wallet);
    runningWei += deltaWei;
    return [
      tr.txHash,                             // A
      tr.bn,                                 // B
      tr.li,                                 // C
      tr.ts,                                 // D
      eventType,                             // E
      counterparty,                          // F
      delta,                                 // G rock.rETH_delta
      Number(runningWei) / WEI_PER_ETH_NUM,  // reserved — becomes col L below
    ];
  });

  const firstDataRow = 2;
  const lastDataRow  = firstDataRow + dataRows.length - 1;

  // Write cols A–G.
  sheet.getRange(firstDataRow, 1, dataRows.length, 7).setValues(dataRows.map(r => r.slice(0, 7)));
  sheet.getRange(firstDataRow, 4, dataRows.length, 1).setNumberFormat(FMT_DATE);
  sheet.getRange(firstDataRow, 7, dataRows.length, 1).setNumberFormat(FMT_18);

  // Col L (rock.rETH_total) — pre-computed from BigInt running sum.
  sheet.getRange(firstDataRow, 12, dataRows.length, 1)
    .setValues(dataRows.map(r => [r[7]]))
    .setNumberFormat(FMT_18);

  // Formulas H, I, J, K
  const rowFormulas = dataRows.map((_, i) => {
    const r = firstDataRow + i;
    return [
      "=IFERROR(VLOOKUP(B" + r + ",'rock.rETH_ratio'!B:H,7,TRUE),\"\")",
      '=IFERROR(VLOOKUP(B' + r + ',rETH_ratio!B:I,8,TRUE),"")',
      '=IF(OR(G' + r + '="",H' + r + '=""),"",G' + r + '*H' + r + ')',
      '=IF(OR(J' + r + '="",I' + r + '=""),"",J' + r + '*I' + r + ')',
    ];
  });
  sheet.getRange(firstDataRow, 8, rowFormulas.length, 4).setFormulas(rowFormulas);
  sheet.getRange(firstDataRow, 8, rowFormulas.length, 4).setNumberFormat(FMT_18);

  // Formulas M, N, O
  const downstreamFormulas = dataRows.map((_, i) => {
    const r = firstDataRow + i;
    return [
      '=IF(OR(L' + r + '="",H' + r + '=""),"",L' + r + '*H' + r + ')',
      '=IF(OR(M' + r + '="",I' + r + '=""),"",M' + r + '*I' + r + ')',
      '=IF(OR(N' + r + '="",SUM(K$2:K' + r + ')=""),"",N' + r + '-SUM(K$2:K' + r + '))',
    ];
  });
  sheet.getRange(firstDataRow, 13, downstreamFormulas.length, 3).setFormulas(downstreamFormulas);
  sheet.getRange(firstDataRow, 13, downstreamFormulas.length, 3).setNumberFormat(FMT_18);

  appendSummaryRow_(sheet, lastDataRow);
  applyFilter_(sheet);
  Logger.log('wallet_' + wallet + ': rebuilt from ' + dataRows.length + ' raw Transfer rows.');
}

// -----------------------------------------------------------------------------
// rebuildAll — rebuilds all derived sheets then refreshes Dashboard
// -----------------------------------------------------------------------------
function rebuildAll(silent) {
  silent = (silent === true);
  rebuildRethRatio();
  rebuildRockRatio();
  rebuildAllWallets();
  rebuildWalletsSummary();
  if (getWalletOptional_()) rebuildWallet();
  refreshDashboard();
  if (!silent) tryAlert_('All derived sheets rebuilt (no API calls used).');
}

// =============================================================================
// Dashboard
// =============================================================================

function refreshDashboard() {
  const ss   = SpreadsheetApp.getActiveSpreadsheet();
  let   dash = ss.getSheetByName(SHEET_DASHBOARD);

  const startDate = dash ? dash.getRange('B5').getValue() : '';
  const endDate   = dash ? dash.getRange('B6').getValue() : '';

  if (!dash) dash = ss.insertSheet(SHEET_DASHBOARD, 0);
  buildDashboardLayout_(dash, startDate, endDate);
  buildCharts_(dash, ss);
}

function buildDashboardLayout_(dash, startDate, endDate) {
  dash.clearContents();

  dash.getRange('A1').setValue('DASHBOARD').setFontSize(16).setFontWeight('bold');
  dash.getRange('A3').setValue('Last updated:');
  dash.getRange('B3').setValue(new Date()).setNumberFormat(FMT_DATE);

  dash.getRange('A5').setValue('Chart from:');
  dash.getRange('B5').setValue(startDate || '').setNumberFormat('dd/mm/yyyy');
  dash.getRange('C5').setValue('(leave blank for all data)').setFontStyle('italic');
  dash.getRange('A6').setValue('Chart to:');
  dash.getRange('B6').setValue(endDate || '').setNumberFormat('dd/mm/yyyy');

  dash.getRange('A8').setValue('rock.rETH/ETH').setFontWeight('bold');
  dash.getRange('A9').setValue('Ratio');
  dash.getRange('B9').setFormula("=IFERROR(LOOKUP(9E+307,'rock.rETH_ratio'!B:B,'rock.rETH_ratio'!N:N),\"\")");
  dash.getRange('C9').setValue('ETH per rock.rETH');
  dash.getRange('A10').setValue('7d APY');
  dash.getRange('B10').setFormula("=IFERROR(LOOKUP(9E+307,'rock.rETH_ratio'!B:B,'rock.rETH_ratio'!Q:Q),\"\")");
  dash.getRange('C10').setFormula('=IF(OR(B10="",B22=""),"", "(" & TEXT(B10-B22,"+0.000%") & " on rETH)")');
  dash.getRange('A11').setValue('30d APY');
  dash.getRange('B11').setFormula("=IFERROR(LOOKUP(9E+307,'rock.rETH_ratio'!B:B,'rock.rETH_ratio'!R:R),\"\")");
  dash.getRange('C11').setFormula('=IF(OR(B11="",B23=""),"", "(" & TEXT(B11-B23,"+0.000%") & " on rETH)")');
  dash.getRange('A12').setValue('Period APY');
  dash.getRange('C12').setFormula('=IF(OR(B12="",B24=""),"", "(" & TEXT(B12-B24,"+0.000%") & " on rETH)")');
  dash.getRange('B9').setNumberFormat('0.000000000');
  dash.getRange('B10:B12').setNumberFormat('0.000%');

  dash.getRange('A14').setValue('rock.rETH/rETH').setFontWeight('bold');
  dash.getRange('A15').setValue('Ratio');
  dash.getRange('B15').setFormula("=IFERROR(LOOKUP(9E+307,'rock.rETH_ratio'!B:B,'rock.rETH_ratio'!H:H),\"\")");
  dash.getRange('C15').setValue('rETH per rock.rETH');
  dash.getRange('A16').setValue('7d APY');
  dash.getRange('B16').setFormula("=IFERROR(LOOKUP(9E+307,'rock.rETH_ratio'!B:B,'rock.rETH_ratio'!O:O),\"\")");
  dash.getRange('A17').setValue('30d APY');
  dash.getRange('B17').setFormula("=IFERROR(LOOKUP(9E+307,'rock.rETH_ratio'!B:B,'rock.rETH_ratio'!P:P),\"\")");
  dash.getRange('A18').setValue('Period APY');
  dash.getRange('B15').setNumberFormat('0.000000000');
  dash.getRange('B16:B18').setNumberFormat('0.000%');

  dash.getRange('A20').setValue('rETH/ETH').setFontWeight('bold');
  dash.getRange('A21').setValue('Ratio');
  dash.getRange('B21').setFormula('=IFERROR(LOOKUP(9E+307,rETH_ratio!B:B,rETH_ratio!I:I),"")');
  dash.getRange('C21').setValue('ETH per rETH');
  dash.getRange('A22').setValue('7d APY');
  dash.getRange('B22').setFormula('=IFERROR(LOOKUP(9E+307,rETH_ratio!B:B,rETH_ratio!K:K),"")');
  dash.getRange('A23').setValue('30d APY');
  dash.getRange('B23').setFormula('=IFERROR(LOOKUP(9E+307,rETH_ratio!B:B,rETH_ratio!L:L),"")');
  dash.getRange('A24').setValue('Period APY');
  dash.getRange('B21').setNumberFormat('0.000000000');
  dash.getRange('B22:B24').setNumberFormat('0.000%');

  dash.getRange('A26').setValue('Fees').setFontWeight('bold');
  dash.getRange('A27').setValue('Total in period (ETH)');
  dash.getRange('C27').setValue('(ETH value of fee dilution minted to protocol)').setFontStyle('italic');
  dash.getRange('B27').setNumberFormat('0.00');
  dash.getRange('A28').setValue('Total in period (rock.rETH)');
  dash.getRange('C28').setValue('(rock.rETH shares minted as fees)').setFontStyle('italic');
  dash.getRange('B28').setNumberFormat('0.00');
  dash.getRange('A29').setValue('Total as % of vault');
  dash.getRange('C29').setValue('(cumulative dilution over the period)').setFontStyle('italic');
  dash.getRange('B29').setNumberFormat('0.000%');
  dash.getRange('A30').setValue('Annualized fee rate');
  dash.getRange('C30').setValue('(period dilution extrapolated to 1 year)').setFontStyle('italic');
  dash.getRange('B30').setNumberFormat('0.000%');
  dash.getRange('A31').setValue('Average fee/day (ETH)');
  dash.getRange('C31').setValue('(total ETH fees in period divided by period days)').setFontStyle('italic');
  dash.getRange('B31').setNumberFormat('0.00');
  dash.getRange('A32').setValue('Average fee/day (rock.rETH)');
  dash.getRange('C32').setValue('(total rock.rETH fees in period divided by period days)').setFontStyle('italic');
  dash.getRange('B32').setNumberFormat('0.00');

  dash.setColumnWidth(1, 160);
  dash.setColumnWidth(2, 120);
  dash.setColumnWidth(3, 260);
}

// _chart_data column layout:
//   1:2   rETH/ETH ratio          4:5   rock.rETH/ETH ratio
//   7:9   rock.rETH/rETH APY      11:13 rock.rETH/ETH APY
//   15:17 ETH fee MAs             19:21 rock.rETH fee MAs
//   23:25 fee rate % MAs          27:28 rock.rETH/rETH ratio
//   30:32 rETH/ETH APY
const CD_COL = {
  RETH_RATIO:    1,
  ROCK_RATIO:    4,
  ROCK_RETH_APY: 7,
  ROCK_ETH_APY:  11,
  FEE_ETH:       15,
  FEE_RETH:      19,
  FEE_PCT:       23,
  ROCK_RETH_RAT: 27,
  RETH_APY:      30,
};

// ── Period-APY tracker helpers (used across sections) ────────────────────────
function mkTracker_() {
  return { sR: null, sT: null, eR: null, eT: null, firstSet: false, firstR: null, firstT: null };
}
function feedTracker_(t, ratio, d, startDate, endDate) {
  if (ratio === '' || ratio === null || !(d instanceof Date)) return;
  if (t.firstR === null) { t.firstR = ratio; t.firstT = d; }
  if (!startDate && !t.firstSet)               { t.sR = ratio; t.sT = d; t.firstSet = true; }
  else if (startDate && d <= startDate)        { t.sR = ratio; t.sT = d; t.firstSet = true; }
  if (!endDate || d <= endDate)                { t.eR = ratio; t.eT = d; }
}
function calcPeriodApy_(t, startDate) {
  if (startDate && !t.firstSet && t.firstR !== null) { t.sR = t.firstR; t.sT = t.firstT; }
  if (!t.sR || !t.eR || t.sR <= 0 || !t.sT || !t.eT || t.sT >= t.eT) return '';
  const days = (t.eT - t.sT) / 86400000;
  return days > 0 ? Math.pow(t.eR / t.sR, 365 / days) - 1 : '';
}

function buildCharts_(dash, ss) {
  const startVal  = dash.getRange('B5').getValue();
  const endVal    = dash.getRange('B6').getValue();
  const startDate = (startVal instanceof Date && !isNaN(startVal.getTime())) ? startVal : null;
  const endDate   = (endVal   instanceof Date && !isNaN(endVal.getTime()))   ? endVal   : null;
  const inRange   = d => d instanceof Date && !isNaN(d.getTime()) &&
                         (!startDate || d >= startDate) && (!endDate || d <= endDate);

  let cd = ss.getSheetByName(SHEET_CHART_DATA);
  if (!cd) { cd = ss.insertSheet(SHEET_CHART_DATA); cd.hideSheet(); }
  cd.clearContents();

  const tReth = mkTracker_(), tRock = mkTracker_(), tEth = mkTracker_();

  const reth = readRethSeries_(ss, inRange, tReth, startDate, endDate);
  const rock = readRockSeries_(ss, inRange, tRock, startDate, endDate);
  const rockEthRatioData = buildMergedRockEthSeries_(
    reth.rethPoints, rock.rockPoints, inRange, tEth, startDate, endDate);
  const fees = buildDailyFeeSeries_(rock.feeEvents, reth.rethPoints, inRange, startDate, endDate);

  const rockRethRatioData = [['blockTimestamp', 'rock.rETH/rETH']];
  for (const p of rock.rockPoints) {
    if (inRange(p.date)) rockRethRatioData.push([p.date, p.rockRethRatio]);
  }

  const sections = [
    { data: reth.rethRatioData,       col: CD_COL.RETH_RATIO                      },
    { data: rockEthRatioData,         col: CD_COL.ROCK_RATIO                      },
    { data: rock.rockRethApyData,     col: CD_COL.ROCK_RETH_APY, pct: true        },
    { data: rock.rockEthApyData,      col: CD_COL.ROCK_ETH_APY,  pct: true        },
    { data: fees.feeEthChartData,     col: CD_COL.FEE_ETH                         },
    { data: fees.feeRethChartData,    col: CD_COL.FEE_RETH                        },
    { data: fees.feePctChartData,     col: CD_COL.FEE_PCT,       pct: true        },
    { data: rockRethRatioData,        col: CD_COL.ROCK_RETH_RAT                   },
    { data: reth.rethApyData,         col: CD_COL.RETH_APY,      pct: true        },
  ];
  const nRows = writeChartData_(cd, sections);

  // KPI cells must always be written, including headless trigger runs where
  // chart APIs can fail due to unavailable UI/charting context.
  writeDashboardKpis_(dash, { tReth, tRock, tEth }, fees, startDate);

  try {
    dash.getCharts().forEach(c => dash.removeChart(c));
    insertCharts_(dash, cd, nRows,
      rockEthRatioData, rockRethRatioData, reth.rethRatioData);
  } catch (e) {
    Logger.log('buildCharts_: chart rendering skipped/failed (KPIs still updated): ' + e.message);
  }
}

// ── rETH/ETH ratio + APY — reads rETH_ratio ──────────────────────────────────
function readRethSeries_(ss, inRange, tReth, startDate, endDate) {
  const rethPoints    = [];
  const rethRatioData = [['blockTimestamp', 'rETH/ETH']];
  const rethApyData   = [['blockTimestamp', '7d', '30d']];
  const sheet         = ss.getSheetByName(SHEET_RETH_RATIO);
  if (!sheet || sheet.getLastRow() <= 1) return { rethPoints, rethRatioData, rethApyData };

  const rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, 12).getValues();
  for (const r of rows) {
    feedTracker_(tReth, r[8], r[3], startDate, endDate);
    if (r[8] === '') continue;
    rethPoints.push({ date: r[3], ratio: r[8] });
    if (!inRange(r[3])) continue;
    rethRatioData.push([r[3], r[8]]);
    const apy7 = r[10], apy30 = r[11]; // cols K, L
    if (apy7 !== '' || apy30 !== '') {
      rethApyData.push([r[3], apy7 === '' ? '' : apy7, apy30 === '' ? '' : apy30]);
    }
  }
  return { rethPoints, rethRatioData, rethApyData };
}

// ── rock.rETH series + fee events — reads rock.rETH_ratio ────────────────────
// Fee mints ('Mint (fees)') are collected on ALL rows; the same-block dedup
// only blanks col H (ratio), but col L (rethAmt) is always set.
function readRockSeries_(ss, inRange, tRock, startDate, endDate) {
  const rockPoints      = [];
  const feeEvents       = [];
  const rockRethApyData = [['blockTimestamp', '7d (rETH)', '30d (rETH)']];
  const rockEthApyData  = [['blockTimestamp', '7d (ETH)',  '30d (ETH)']];
  const sheet = ss.getSheetByName(SHEET_ROCK_RATIO);
  if (!sheet || sheet.getLastRow() <= 1) {
    return { rockPoints, feeEvents, rockRethApyData, rockEthApyData };
  }

  const rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, 18).getValues();
  for (const r of rows) {
    const h = r[7]; // col H — rock.rETH/rETH
    feedTracker_(tRock, h, r[3], startDate, endDate);
    if (r[4] === 'Mint (fees)' && Number(r[11]) > 0) {
      const feeReth = Number(r[11]), supply = Number(r[6]);
      feeEvents.push({
        date: r[3], feeReth, totalSupply: supply,
        feePct: supply > 0 ? feeReth / supply : 0,
      });
    }
    if (h === '') continue;
    // Cols (0-indexed): M=12 rETH/ETH, N=13 rock.rETH/ETH, O=14 apy_7d_rETH,
    //                   P=15 apy_30d_rETH, Q=16 apy_7d_ETH, R=17 apy_30d_ETH
    const [n, o, p, q] = [r[14], r[15], r[16], r[17]];
    rockPoints.push({ date: r[3], rockRethRatio: h, n, o, p, q });
    if (!inRange(r[3])) continue;
    if (n !== '' || o !== '') rockRethApyData.push([r[3], n === '' ? '' : n, o === '' ? '' : o]);
    if (p !== '' || q !== '') rockEthApyData.push( [r[3], p === '' ? '' : p, q === '' ? '' : q]);
  }
  return { rockPoints, feeEvents, rockRethApyData, rockEthApyData };
}

// ── Merged rock.rETH/ETH series ──────────────────────────────────────────────
// Interleave rETH/ETH change events with rock.rETH/rETH change events. At each
// event, update the relevant component and emit a new rock.rETH/ETH point.
function buildMergedRockEthSeries_(rethPoints, rockPoints, inRange, tEth, startDate, endDate) {
  const rockRatioData = [['blockTimestamp', 'rock.rETH/ETH']];
  const merged = [
    ...rethPoints.map(p => ({ date: p.date, type: 'reth', ratio: p.ratio })),
    ...rockPoints.map(p => ({ date: p.date, type: 'rock', ratio: p.rockRethRatio })),
  ].sort((a, b) => a.date - b.date);

  let lastReth = 0, lastRock = 0;
  for (const pt of merged) {
    if (pt.type === 'reth') lastReth = pt.ratio;
    else                    lastRock = pt.ratio;
    if (lastReth > 0 && lastRock > 0) {
      const rockEthRatio = lastRock * lastReth;
      feedTracker_(tEth, rockEthRatio, pt.date, startDate, endDate);
      if (inRange(pt.date)) rockRatioData.push([pt.date, rockEthRatio]);
    }
  }
  return rockRatioData;
}

// ── Daily fee moving averages + period totals ────────────────────────────────
function buildDailyFeeSeries_(feeEvents, rethPoints, inRange, startDate, endDate) {
  // Convert fee shares (rETH) to ETH via rETH/ETH ratio at that date.
  let rpIdx = 0, lastRethForFee = 0;
  for (const fe of feeEvents) {
    while (rpIdx < rethPoints.length && rethPoints[rpIdx].date <= fe.date) {
      lastRethForFee = rethPoints[rpIdx].ratio;
      rpIdx++;
    }
    fe.feeEth = lastRethForFee > 0 ? fe.feeReth * lastRethForFee : fe.feeReth;
  }

  // Group fee events into calendar days (local midnight).
  const dayMap = new Map();
  for (const fe of feeEvents) {
    const d = new Date(fe.date);
    d.setHours(0, 0, 0, 0);
    const key = d.getTime();
    if (!dayMap.has(key)) dayMap.set(key, { date: d, totalEth: 0, totalReth: 0, totalPct: 0 });
    const day = dayMap.get(key);
    day.totalEth  += fe.feeEth;
    day.totalReth += fe.feeReth;
    day.totalPct  += fe.feePct;
  }
  const dailyFees = Array.from(dayMap.values()).sort((a, b) => a.date - b.date);

  const feeEthChartData  = [['blockTimestamp', '7d avg (ETH/day)',       '30d avg (ETH/day)']];
  const feeRethChartData = [['blockTimestamp', '7d avg (rock.rETH/day)', '30d avg (rock.rETH/day)']];
  const feePctChartData  = [['blockTimestamp', '7d annualized',          '30d annualized']];
  let totalFeesEthInPeriod = 0, totalFeesRethInPeriod = 0, totalFeesPctInPeriod = 0;
  let firstFeeDate = null, lastFeeDate = null;

  for (const { date, totalEth, totalReth, totalPct } of dailyFees) {
    const t   = date.getTime();
    const t7  = t - 6  * 86400000;
    const t30 = t - 29 * 86400000;
    let sumEth7 = 0, sumEth30 = 0, sumReth7 = 0, sumReth30 = 0, sumPct7 = 0, sumPct30 = 0;
    for (const df of dailyFees) {
      const dt = df.date.getTime();
      if (dt >= t7  && dt <= t) { sumEth7  += df.totalEth;  sumReth7  += df.totalReth;  sumPct7  += df.totalPct; }
      if (dt >= t30 && dt <= t) { sumEth30 += df.totalEth;  sumReth30 += df.totalReth;  sumPct30 += df.totalPct; }
    }
    if (inRange(date)) {
      // MA(N) = sum of N preceding calendar days / N (days with no events count as 0).
      feeEthChartData.push( [date, sumEth7  / 7,       sumEth30  / 30]);
      feeRethChartData.push([date, sumReth7 / 7,       sumReth30 / 30]);
      feePctChartData.push( [date, sumPct7  / 7 * 365, sumPct30  / 30 * 365]);
      totalFeesEthInPeriod  += totalEth;
      totalFeesRethInPeriod += totalReth;
      totalFeesPctInPeriod  += totalPct;
      if (!firstFeeDate) firstFeeDate = date;
      lastFeeDate = date;
    }
  }

  const annualizationDays = firstFeeDate && lastFeeDate && lastFeeDate > firstFeeDate
    ? (lastFeeDate - firstFeeDate) / 86400000
    : 365; // fallback: 1-year period when only one day of fees

  let avgDays = 0;
  if (startDate && endDate && endDate >= startDate) {
    avgDays = Math.max(1, Math.floor((endDate - startDate) / 86400000) + 1);
  } else if (startDate && !endDate) {
    const endBound = lastFeeDate || startDate;
    avgDays = Math.max(1, Math.floor((endBound - startDate) / 86400000) + 1);
  } else if (!startDate && endDate) {
    const startBound = firstFeeDate || endDate;
    avgDays = Math.max(1, Math.floor((endDate - startBound) / 86400000) + 1);
  } else if (firstFeeDate && lastFeeDate) {
    avgDays = Math.max(1, Math.floor((lastFeeDate - firstFeeDate) / 86400000) + 1);
  }

  const annualizedFeeRate = totalFeesPctInPeriod * 365 / annualizationDays;
  const avgFeeEthPerDay   = avgDays > 0 ? totalFeesEthInPeriod  / avgDays : '';
  const avgFeeRethPerDay  = avgDays > 0 ? totalFeesRethInPeriod / avgDays : '';

  return {
    feeEthChartData, feeRethChartData, feePctChartData,
    totalFeesEthInPeriod, totalFeesRethInPeriod, totalFeesPctInPeriod,
    annualizedFeeRate, avgFeeEthPerDay, avgFeeRethPerDay,
  };
}

// ── Write _chart_data blocks ─────────────────────────────────────────────────
function writeChartData_(cd, sections) {
  const nRows = {};
  for (const { data, col, pct } of sections) {
    const w = data[0].length;
    cd.getRange(1, col, data.length, w).setValues(data);
    if (data.length > 1) {
      cd.getRange(2, col, data.length - 1, 1).setNumberFormat(FMT_DATE);
      if (pct) cd.getRange(2, col + 1, data.length - 1, w - 1).setNumberFormat('0.0%');
    }
    nRows[col] = data.length;
  }
  return nRows;
}

// ── Chart helpers ────────────────────────────────────────────────────────────
function applyChartOpts_(cb, opts) {
  for (const k in opts) cb = cb.setOption(k, opts[k]);
  return cb;
}
function baseChartOpts_(title, vTitle) {
  return {
    title, 'hAxis.title': 'Date', 'vAxis.title': vTitle,
    'legend.position': 'bottom', width: 580, height: 350,
  };
}
// Single-series ratio charts: no legend, tight Y bounds.
function ratioAxisBounds_(data) {
  const vals = data.slice(1).map(r => r[1]).filter(v => typeof v === 'number' && !isNaN(v));
  if (vals.length === 0) return null;
  const lo = Math.min(...vals), hi = Math.max(...vals);
  const pad = (hi - lo) * 0.1 || hi * 0.001;
  return { min: lo - pad, max: hi + pad };
}
function ratioChartOpts_(title, vTitle, data) {
  const opts = { ...baseChartOpts_(title, vTitle), 'legend.position': 'none' };
  const b = ratioAxisBounds_(data);
  if (b) { opts['vAxis.viewWindow.min'] = b.min; opts['vAxis.viewWindow.max'] = b.max; }
  return opts;
}

// ── Insert the 3×3 chart grid ────────────────────────────────────────────────
function insertCharts_(dash, cd, nRows, rockRatioData, rockRethRatioData, rethRatioData) {
  // Row anchors (R1=row1, R2=row18, R3=row35) and col anchors (C1=5, C2=11, C3=17).
  // Each chart is 580px × 350px → roughly 6 cols × 17 rows.
  const R1 = 1, R2 = 18, R3 = 35;
  const C1 = 5, C2 = 11, C3 = 17;

  const build = (col, width, opts, pos) => {
    dash.insertChart(applyChartOpts_(
      dash.newChart().setChartType(Charts.ChartType.LINE)
        .addRange(cd.getRange(1, col, nRows[col], width)).setNumHeaders(1),
      opts
    ).setPosition(pos[0], pos[1], 0, 0).build());
  };

  // Row 1 — ratio history.
  build(CD_COL.ROCK_RATIO,    2,
    ratioChartOpts_('rock.rETH/ETH Ratio History',  'ETH per rock.rETH',  rockRatioData),
    [R1, C1]);
  build(CD_COL.ROCK_RETH_RAT, 2,
    ratioChartOpts_('rock.rETH/rETH Ratio History', 'rETH per rock.rETH', rockRethRatioData),
    [R1, C2]);
  build(CD_COL.RETH_RATIO,    2,
    ratioChartOpts_('rETH/ETH Ratio History',       'ETH per rETH',       rethRatioData),
    [R1, C3]);

  // Row 2 — APY.
  build(CD_COL.ROCK_ETH_APY,  3,
    { ...baseChartOpts_('rock.rETH/ETH APY',  'APY'), 'vAxis.format': '0.0%' },
    [R2, C1]);
  build(CD_COL.ROCK_RETH_APY, 3,
    { ...baseChartOpts_('rock.rETH/rETH APY', 'APY'), 'vAxis.format': '0.0%' },
    [R2, C2]);
  if (nRows[CD_COL.RETH_APY] > 1) {
    build(CD_COL.RETH_APY,    3,
      { ...baseChartOpts_('rETH/ETH APY', 'APY'), 'vAxis.format': '0.0%' },
      [R2, C3]);
  }

  // Row 3 — fees.
  if (nRows[CD_COL.FEE_PCT] > 1) {
    build(CD_COL.FEE_PCT, 3,
      { ...baseChartOpts_('Fee Rate — Annualized Moving Avg', '% of vault / year'),
        'vAxis.format': '0.000%' },
      [R3, C1]);
  }
  if (nRows[CD_COL.FEE_RETH] > 1) {
    build(CD_COL.FEE_RETH, 3,
      baseChartOpts_('Daily Fees — Moving Avg (rock.rETH)', 'rock.rETH / day'),
      [R3, C2]);
  }
  if (nRows[CD_COL.FEE_ETH] > 1) {
    build(CD_COL.FEE_ETH, 3,
      baseChartOpts_('Daily Fees — Moving Avg (ETH)', 'ETH / day'),
      [R3, C3]);
  }
}

// ── Write the period-APY and fee-summary KPI cells ───────────────────────────
function writeDashboardKpis_(dash, trackers, fees, startDate) {
  dash.getRange('B12').setValue(calcPeriodApy_(trackers.tEth,  startDate));
  dash.getRange('B18').setValue(calcPeriodApy_(trackers.tRock, startDate));
  dash.getRange('B24').setValue(calcPeriodApy_(trackers.tReth, startDate));
  dash.getRange('B27').setValue(fees.totalFeesEthInPeriod  ?? '');
  dash.getRange('B28').setValue(fees.totalFeesRethInPeriod ?? '');
  dash.getRange('B29').setValue(fees.totalFeesPctInPeriod  ?? '');
  dash.getRange('B30').setValue(fees.annualizedFeeRate     ?? '');
  dash.getRange('B31').setValue(fees.avgFeeEthPerDay       ?? '');
  dash.getRange('B32').setValue(fees.avgFeeRethPerDay      ?? '');
}

// =============================================================================
// Summary row helper
// =============================================================================

// Writes a SUMMARY row below the data block of a wallet sheet.
// Layout mirrors wallet_<addr>: cols H..O are filled with ratio/value formulas,
// col L pulls the pre-computed BigInt-derived rock.rETH_total from the last
// data row (so closed positions show exactly 0).
function appendSummaryRow_(sheet, lastDataRow) {
  const ldr = lastDataRow || sheet.getLastRow();
  const s   = ldr + 2;

  sheet.getRange(s, 1).setValue('SUMMARY');
  sheet.getRange(s, 4).setValue(new Date()).setNumberFormat(FMT_DATE);

  const rockTotalFormula = ldr < 2 ? '=0' : '=L' + ldr;

  sheet.getRange(s, 8, 1, 8).setFormulas([[
    "=LOOKUP(9E+307,'rock.rETH_ratio'!B:B,'rock.rETH_ratio'!H:H)",
    '=LOOKUP(9E+307,rETH_ratio!B:B,rETH_ratio!I:I)',
    '=SUM(J$2:J' + ldr + ')',
    '=SUM(K$2:K' + ldr + ')',
    rockTotalFormula,
    '=IF(OR(L' + s + '="",H' + s + '=""),"",L' + s + '*H' + s + ')',
    '=IF(OR(M' + s + '="",I' + s + '=""),"",M' + s + '*I' + s + ')',
    '=IF(OR(N' + s + '="",K' + s + '=""),"",N' + s + '-K' + s + ')',
  ]]);
  sheet.getRange(s, 8, 1, 8).setNumberFormat(FMT_18);
  sheet.getRange(s, 1, 1, 15).setFontWeight('bold');
}
