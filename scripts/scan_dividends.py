#!/usr/bin/env python3
"""
Scan Indian Large Cap (Nifty 100) stocks for dividend yield and annual dividend payouts.
"""

from __future__ import annotations

import argparse
import csv
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from rich.console import Console
from rich.table import Table
import yfinance as yf

from honba.markets.india.universes import NIFTY_50_SYMBOLS


def get_large_cap_symbols() -> list[str]:
    url = "https://nsearchives.nseindia.com/content/indices/ind_nifty100list.csv"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            reader = csv.DictReader(resp.read().decode("utf-8").splitlines())
            symbols = [r["Symbol"].strip() for r in reader if "Symbol" in r and r["Symbol"].strip()]
            if len(symbols) >= 50:
                return sorted(set(symbols))
    except Exception:
        pass
    return sorted(set(NIFTY_50_SYMBOLS))


def fetch_symbol_dividend(sym: str) -> dict | None:
    try:
        t = yf.Ticker(f"{sym}.NS")
        inf = t.info
        dy = inf.get("dividendYield")
        rate = inf.get("dividendRate")
        price = inf.get("currentPrice") or inf.get("regularMarketPrice") or inf.get("previousClose")
        name = inf.get("shortName") or sym
        if dy is not None and dy > 0:
            return {
                "symbol": sym,
                "name": name,
                "price": price,
                "dividend_yield": dy,
                "dividend_rate": rate or 0.0,
            }
    except Exception:
        pass
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan large cap stocks for dividend yields")
    parser.add_argument("--min-yield", type=float, default=2.0, help="Minimum dividend yield percentage (default: 2.0)")
    parser.add_argument("--limit", type=int, default=50, help="Max results to display (default: 50)")
    args = parser.parse_args()

    console = Console()
    symbols = get_large_cap_symbols()
    console.print(f"[cyan]Scanning {len(symbols)} Large Cap stocks for dividend yield >= {args.min_yield}%...[/cyan]")

    with ThreadPoolExecutor(max_workers=10) as ex:
        results = [r for r in ex.map(fetch_symbol_dividend, symbols) if r]

    # Filter & Sort
    filtered = [r for r in results if r["dividend_yield"] >= args.min_yield]
    filtered.sort(key=lambda x: x["dividend_yield"], reverse=True)
    displayed = filtered[: args.limit]

    table = Table(title=f"Large Cap Dividend Stocks ({len(displayed)} matched, yield >= {args.min_yield}%)")
    table.add_column("Symbol", style="cyan", no_wrap=True)
    table.add_column("Company", style="white")
    table.add_column("CMP (₹)", justify="right", style="green")
    table.add_column("Div Yield (%)", justify="right", style="bold yellow")
    table.add_column("Annual Div/Share (₹)", justify="right", style="magenta")

    for r in displayed:
        table.add_row(
            f"{r['symbol']}.NSE",
            r["name"][:28],
            f"{r['price']:.2f}" if r["price"] else "-",
            f"{r['dividend_yield']:.2f}%",
            f"₹{r['dividend_rate']:.2f}",
        )

    console.print(table)


if __name__ == "__main__":
    main()
