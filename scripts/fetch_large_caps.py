#!/usr/bin/env python3
"""
Convert existing cached NSE Bhavcopies to Parquet, remove the Bhavcopy cache,
and fetch 10 years of daily data for the Large Cap (Nifty 100) universe.
"""

from __future__ import annotations

import csv
import datetime as dt
import lzma
import time
import urllib.request
from pathlib import Path

from honba.data.loaders import parse_bhavcopy_csv
from honba.data.loaders.yfinance import YFinanceProvider
from honba.entities.instrument import InstrumentId
from honba.markets.india.universes import NIFTY_50_SYMBOLS
from honba.screener.coverage import CoverageRecord, CoverageStatus, DateInterval
from honba.screener.service import DataService
from honba.screener.store import ParquetBarStore


def get_large_cap_symbols() -> list[str]:
    """Fetch official Nifty 100 constituents list, falling back to Nifty 50."""
    url = "https://nsearchives.nseindia.com/content/indices/ind_nifty100list.csv"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            reader = csv.DictReader(resp.read().decode("utf-8").splitlines())
            symbols = [r["Symbol"].strip() for r in reader if "Symbol" in r and r["Symbol"].strip()]
            if len(symbols) >= 50:
                return sorted(set(symbols))
    except Exception as e:
        print(f"Warning: could not fetch online Nifty 100 list ({e}); using Nifty 50.")
    return sorted(set(NIFTY_50_SYMBOLS))


def main() -> None:
    store = ParquetBarStore()
    bhav_dir = store.data_dir / "cache" / "bhavcopy"
    bhav_files = sorted(bhav_dir.glob("bhavcopy_*.csv.xz"))

    symbols = get_large_cap_symbols()
    print(f"Targeting {len(symbols)} Large Cap symbols.")

    # 1. Convert cached Bhavcopies for target symbols into Parquet
    if bhav_files:
        print(f"Step 1: Converting {len(bhav_files)} cached Bhavcopy files for Large Caps...")
        symbol_set = set(symbols)
        bars_by_symbol: dict[str, list] = {}

        t0 = time.time()
        for fpath in bhav_files:
            try:
                with lzma.open(fpath, "rt", encoding="utf-8") as f:
                    day_bars = parse_bhavcopy_csv(f.read())
                    for sym, bar in day_bars.items():
                        if sym in symbol_set:
                            bars_by_symbol.setdefault(sym, []).append(bar)
            except Exception as exc:
                print(f"  Warning reading {fpath.name}: {exc}")

        # Ingest into store
        ingested_count = 0
        for sym, bars in bars_by_symbol.items():
            if not bars:
                continue
            dates = [dt.datetime.fromtimestamp(b.ts / 1e9, tz=dt.timezone.utc).date() for b in bars]
            min_d, max_d = min(dates), max(dates)
            record = CoverageRecord(
                exchange="NSE",
                symbol=sym,
                timeframe="1D",
                interval=DateInterval(min_d, max_d + dt.timedelta(days=1)),
                status=CoverageStatus.FINAL,
                source="nse_bhavcopy",
                row_count=len(bars),
                fetched_at_ns=int(time.time() * 1e9),
            )
            store.append(record, bars)
            ingested_count += len(bars)

        print(f"  Converted {ingested_count} bars across {len(bars_by_symbol)} symbols in {time.time() - t0:.2f}s.")

        # 2. Remove bhavcopy cache files
        print(f"Step 2: Removing {len(bhav_files)} cached Bhavcopy files...")
        removed_count = 0
        for fpath in bhav_files:
            try:
                fpath.unlink(missing_ok=True)
                removed_count += 1
            except Exception as e:
                print(f"  Error deleting {fpath.name}: {e}")
        print(f"  Successfully deleted {removed_count} Bhavcopy files from cache.")
    else:
        print("Step 1 & 2: No Bhavcopy cache files to convert/remove.")

    # 3. Fetch 10 years of daily data via YFinanceProvider
    print(f"\nStep 3: Fetching 10 years of daily data for {len(symbols)} Large Cap stocks...")
    provider = YFinanceProvider()
    service = DataService(store=store, providers=[provider])

    start_date = dt.date(2014, 1, 1)
    end_date = dt.date(2026, 10, 10)
    req_interval = DateInterval(start_date, end_date)

    success_count = 0
    fail_count = 0
    total_bars_fetched = 0

    t_start = time.time()
    for idx, sym in enumerate(symbols, 1):
        inst = InstrumentId(sym, "NSE")
        t_sym = time.time()
        try:
            plan = service.plan([inst], "1D", req_interval)
            gaps = plan.gaps_by_instrument.get(inst, [])
            if gaps:
                res = service.ensure(plan)
                bars = res.bars.get(inst, [])
                total_bars_fetched += len(bars)
                success_count += 1
                print(f"[{idx:3d}/{len(symbols)}] {sym:12s} -> {len(bars):4d} bars ({time.time() - t_sym:.2f}s)")
            else:
                bars = store.read(inst, "1D", req_interval)
                total_bars_fetched += len(bars)
                success_count += 1
                print(f"[{idx:3d}/{len(symbols)}] {sym:12s} -> already complete ({len(bars)} bars)")
        except Exception as exc:
            fail_count += 1
            print(f"[{idx:3d}/{len(symbols)}] {sym:12s} -> FAILED: {exc}")

    elapsed = time.time() - t_start
    print(f"\nFetch complete in {elapsed:.1f}s!")
    print(f"Success: {success_count}/{len(symbols)} symbols")
    if fail_count:
        print(f"Failed: {fail_count} symbols")
    print(f"Total bars stored in catalog: {total_bars_fetched}")


if __name__ == "__main__":
    main()
