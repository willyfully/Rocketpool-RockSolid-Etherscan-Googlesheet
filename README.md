# RockSolid Etherscan Script

Google Apps Script for Google Sheets that tracks the Rocket Pool ecosystem and RockSolid vault positions directly from on-chain events.

It pulls Ethereum logs from Etherscan, stores raw event history in append-only sheets, and rebuilds derived analytics (ratios, APY, wallet P&L, dashboard charts) without extra API calls.

**Security warning**: this is a Google Apps Script that runs with your Google account permissions. You must do your own code review before using it, then only grant authorizations if you understand and trust what it does. Unfortunately there is no way of limiting the scope of authorizations to just one workbook on Apps Script.

Requires an Etherscan API key (free tier is enough for normal usage).

## What this script does

- Gives you a live `Dashboard` with:
  - `rETH/ETH`, `rock.rETH/rETH`, and `rock.rETH/ETH` charts and latest values
  - 7d/30d APY charts and latest values, average APY on a selected period
  - 7d/30d average protocol fees charts and latest values, annualized fees rate, average fees per day
- Tracks wallet outcomes:
  - personal sheet `wallet_<address>` with position value and gain/loss
  - ecosystem-wide views in `wallets_all` and `wallets_summary` with position value and gain/loss
- Supports hourly auto-update trigger

## Setup

1. Open your Google Sheet, then go to **Extensions -> Apps Script**.
2. Paste contents of `RockSolid-Etherscan.gs.js` and save.
3. Review the script before running it.
4. Reload the spreadsheet.
5. You will be asked for authorizations, review them
6. Use menu **Rocket Pool -> Set Etherscan API key...** (free tier key works).
7. (Optional) Use **Rocket Pool -> Set tracked wallet address...**.
8. Run **Rocket Pool -> Import all (API calls)**.

After initial import, run:

- **Import all (API calls)** for incremental updates
- **Rebuild all derived sheets (no API)** to recompute everything locally

## Menu actions

From the **Rocket Pool** menu:

- API/setup:
  - `Set Etherscan API key...`
  - `Set tracked wallet address...`
- Imports:
  - `Import all (API calls)`
  - `Import: Rocket Pool events`
  - `Import: RockSolid Vault events`
- Rebuilds:
  - `Rebuild all derived sheets (no API)`
  - `Rebuild: rETH ratio`
  - `Rebuild: rock.rETH ratio`
  - `Rebuild: all wallets sheet`
  - `Rebuild: wallets summary`
  - `Rebuild: wallet sheet (tracked)`
- Dashboard and automation:
  - `Refresh Dashboard`
  - `Enable hourly auto-update`
  - `Disable auto-update`
  - `Auto-update status...`

## Architecture

### Two-phase design

1. **Import phase** (API calls): writes only raw event sheets.
2. **Rebuild phase** (no API calls): computes all derived sheets from raw data.

This lets you:

- run normal updates (`importAll`) when API quota is available
- run rebuild-only updates (`rebuildAll`) when you want to recalculate formulas or avoid API calls

### Data tiers

**Tier 1: Raw sheets**

- `RP_evt_BalancesUpdated`
- `RS_evt_TotalAssetsUpdated`
- `RS_evt_SettleRedeem`
- `RS_evt_Transfer`

**Tier 2: Derived sheets**

- `rETH_ratio`
- `rock.rETH_ratio`
- `wallets_all`
- `wallets_summary`
- `wallet_<tracked_wallet>`
- `Dashboard`

## How ratios are computed

### `rETH/ETH`

From Rocket Pool `BalancesUpdated`:

`ratio = totalEth / rethSupply`

### `rock.rETH/rETH`

Built by merging three event streams in block/log order:

- `SettleRedeem`: hard resync of both `totalAssets` and `totalSupply`
- `TotalAssetsUpdated`: oracle update for `totalAssets`
- `Transfer` mint/burn events: updates supply and adjusts assets proportionally between oracle updates

Fee mints are detected and treated as dilution events.

## Contracts queried

- RockSolid `rock.rETH` proxy: `0x936faCdf10c8c36294e7b9d28345255539d81bc7`
- Rocket Pool `rocketNetworkBalances`:
  - `0x138313f102ce9a0662f826fca977e3ab4d6e5539` (v1)
  - `0x07FCaBCbe4ff0d80c2b1eb42855C0131b6cba2F4` (v2)
  - `0x6Cc65bF618F55ce2433f9D8d827Fc44117D81399` (v3)
  - `0x1D9F14C6Bfd8358b589964baD8665AdD248E9473` (v4)

## Notes and limitations

- Designed for Google Apps Script V8 runtime.
- Uses BigInt parsing via `BigInt("...")` for integer safety (not BigInt literals like `0n`).
- Raw import sheets are append-only and cursor-based (`block_num`, `log_idx`) for safe repeat runs.
- Rebuild functions can be run independently at any time.

## Typical workflow

1. Import new logs (`Import all`)
2. Rebuild derived analytics (`Rebuild all`)
3. Review KPIs and charts on `Dashboard`
4. Inspect global wallets in `wallets_all` / `wallets_summary`
5. Track personal position in `wallet_<address>`
