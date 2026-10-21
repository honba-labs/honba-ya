# Handoff: `honba serve` isn't serving real market data

## Symptom
honba-kao's workbench charts (now correctly wired to fetch real data — see
`honba-kao/vendor/openalgo-charts/honba-bridge.js`'s `honbaFeed`) get
`market_data_unavailable` / `instrument_not_found` for every symbol tried,
even ones that should have data. `GET /instruments?exchange=NSE` only
returns 5 symbols: BANKBARODA, INFY, RELIANCE, TCS, TITAN.

## Root cause
The running server was started as:
```
target/release/honba serve --data-dir ../honba-data/data/
```
`AppState::from_parquet_dir(dir)` (`crates/honba-ya-api-rest/src/state.rs:292`)
expects **flat** `SYMBOL.EXCHANGE.parquet` files directly inside `dir` — one
file per instrument. `../honba-data/data/` does contain exactly 5 such flat
files (`INFY.NSE.parquet`, `TITAN.NSE.parquet`, `BANKBARODA.NSE.parquet`,
`RELIANCE.NSE.parquet`, `TCS.NSE.parquet`) — these look like old manual/smoke-
test fixtures, not real ingested data, and that's why exactly those 5 and
only those 5 show up in `/instruments`. Even they fail `read_bars` for every
timeframe tried (1d, 5m) — the fixture files apparently don't cover those
windows.

The **real** data lives in `honba-data/data/catalog/<TF>/<EXCHANGE>/<SYMBOL>/...`
(nested per-timeframe, per-exchange, per-symbol — e.g.
`catalog/1D/NSE/HDFCBANK/...`, `catalog/5M/NSE/RELIANCE/...`), with dozens of
symbols present (confirmed on disk — HDFCBANK is one of them, which is why it
404s as `instrument_not_found` today: it has no flat fixture file). This tree
is `honba-data`'s own output format and `from_parquet_dir`/`DatasetReader`
has no code path that reads it.

## What's unresolved (needs a decision, not a guess-fix)
Per the architecture understood from `honba-data`'s own source (providers/
store/live) and `honba-ya-cli`'s `data ticks-import`/`bar_pump.rs`: honba-data
is meant to be an ingestion/storage layer, with some honba-ya-side step
converting its output into whatever `honba-ya-api-rest` actually serves from.
It's not clear from this investigation alone whether:
1. There's an existing `honba-ya-cli` subcommand meant to flatten/convert
   `honba-data`'s `catalog/<TF>/<EXCHANGE>/<SYMBOL>/` tree into the flat
   `SYMBOL.EXCHANGE.parquet` layout `from_parquet_dir` wants (and it was
   simply never run against this dataset), or
2. `from_parquet_dir`/`DatasetReader` needs a new reader variant that
   understands the nested catalog layout directly (so the conversion step
   isn't needed at all), or
3. `honba serve` should just be pointed at a *different*, already-converted
   directory that doesn't exist yet in this checkout.

Whoever picks this up should start by checking `honba-ya-cli`'s subcommands
(`honba data --help`, `honba --help`) for anything that looks like a
catalog→flat-parquet conversion step, and check whether `honba-data`'s own
CLI/service has an export mode aimed at this format.

## How to reproduce / verify a fix
```
curl -s 'http://127.0.0.1:8000/instruments?exchange=NSE' | jq '.data.instruments | length'
curl -s 'http://127.0.0.1:8000/bars/HDFCBANK.NSE?tf=1d' | jq .
```
Once fixed, both should return real data; honba-kao's workbench needs no
further changes — the feed (`honba-bridge.js`) and the mount pipeline
(`pane_chart.rs`/`openalgo_chart.rs`) are already correct and verified
end-to-end against this same backend.

— written 2026-10-20, from a honba-kao session investigating "workbench has
no charts" at the user's request.
