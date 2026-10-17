"""Generates ``schema/conformance/l2_sim.json``: conformance vectors for the L2 simulator (ADR 0030).

Unlike the L1 generator nothing is run to produce the expectations: the Python reference
(``honba.backtest.l2_sim``) is a second implementation checked against them, not their source.
Every expected fill, rejection, cash and position below is HAND-DERIVED from the ADR 0030 section 3
rules and written next to its scenario; nothing is recorded from ``honba_sim::L2Sim``. The Rust
replay (``crates/honba-sim/tests/l2_conformance.rs``) is the first consumer; the pyclass replay
(``crates/honba-py/tests/l2_native.rs``) and the Python replays
(``python/tests/integration/test_l2_sim_conformance.py``, both backends) read the same file, so
the schema below is the contract.

Run from the repo root: ``python3 scripts/gen_l2_vectors.py`` (``--check`` only compares, exit 1 on
drift; the Rust test runs it, and ``make check-l2-vectors`` does too). Deterministic: no wall
clock, no randomness, stdlib only.

Scenario schema (``fixture_version`` 1)::

    {"name", "chunk", "config": {"cash", "currency", "long_only", "lot_sizes", "costs"?},
     "steps": [...], "expect": {"drains": [...], "final": {...}}}

``chunk`` is 1 for every scenario here (a runner replays the chunks it implements).
``cash`` is integer minor units, ``currency`` is ``"INR"``. ``long_only`` caps a sell at the
position held. ``lot_sizes`` maps symbol -> quantity step. ``config.costs`` is the same test cost
model as the L1 vectors: ``{"flat_buy", "flat_sell", "bps_buy", "bps_sell"}`` (optional integers,
minor units); the cost of a fill is ``flat + (notional_minor * bps + 5000) // 10000``, where
``notional_minor`` is ``round(qty * price * 100)``. Symbols are NSE instruments. All times are
integer nanoseconds (event time; there is no wall clock). The matching rules are those of the
``honba_sim::l2`` module docs. Steps, replayed in order:

* ``{"op": "quote", "symbol", "ts", "bid", "ask", "bid_size", "ask_size"}``: a ``QuoteTick``.
* ``{"op": "trade", "symbol", "ts", "price", "size"}``: a ``TradeTick`` (``aggressor`` optional:
  ``"buyer"`` default, ``"seller"``, ``"none"``; it does not affect matching).
* ``{"op": "submit", "id", "symbol", "side": "buy"|"sell", "type": "market"|"limit"|
  "stop_market"|"stop_limit"|"trailing_stop", "qty", "ts", "price"?, "trigger"?,
  "tif"?: "day"(default)|"gtc"|"ioc"|"fok"}``
* ``{"op": "cancel", "id", "now"}``: ``now`` is the engine time of the cancel and stamps the
  cancellation (ADR 0019 decision 6, L2 v1); cancelling an unknown id is a no-op.
* ``{"op": "advance_ts", "ts"}``: move event time with no market event (``L2Sim::advance_to``);
  Day orders whose session close has passed expire, stamped at their close instant.
* ``{"op": "session_close", "ts"}``: explicit session close (``L2Sim::on_session_close``); every
  working Day order expires, stamped ``ts``.
* ``{"op": "set_position", "symbol", "qty"}``: seed a position (a harness step, not an event).
* ``{"op": "drain"}``: drain fills and rejections; recorded in ``expect.drains``.

A step that raises gets ``"error": true`` (none do today). ``expect.drains`` is a list of
``{"fills", "rejections"}``, one per ``drain`` step. A fill is
``[order_id, symbol, side, quantity, price, ts, costs]`` and a rejection
``[order_id, symbol, side, quantity, reason, ts]`` (quantity = the unfilled remainder). A
cancellation and an expiry are rejections with reason ``"cancelled"`` / ``"expired"``, exactly as
the legacy drain pair surfaces them for L1. ``expect.final`` has ``cash``, ``fees``,
``traded_notional`` (minor units), ``positions`` (symbol -> signed quantity; flat symbols are
absent) and ``working`` (ids of orders still working, in submission order). Numbers compare by
value (``10`` equals ``10.0``).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

OUT = Path(__file__).resolve().parents[1] / "schema" / "conformance" / "l2_sim.json"
FIXTURE_VERSION = 1

# 23:59:00 UTC into the day: the default Day-order session close (ADR 0030 / `l2` module docs).
CLOSE = 86_340_000_000_000
DAY = 86_400_000_000_000


# ---- step builders -------------------------------------------------------------------------


def quote(ts: int, bid: float, ask: float, bid_size: float, ask_size: float, sym: str = "AAA"):
    return {
        "op": "quote", "symbol": sym, "ts": ts, "bid": bid, "ask": ask,
        "bid_size": bid_size, "ask_size": ask_size,
    }  # fmt: skip


def trade(ts: int, price: float, size: float, sym: str = "AAA") -> dict[str, Any]:
    return {"op": "trade", "symbol": sym, "ts": ts, "price": price, "size": size}


def submit(
    oid: str, side: str, typ: str, qty: float, ts: int, sym: str = "AAA", **kw: Any
) -> dict[str, Any]:
    step: dict[str, Any] = {
        "op": "submit", "id": oid, "symbol": sym, "side": side, "type": typ, "qty": qty, "ts": ts,
    }  # fmt: skip
    step.update(kw)  # price, trigger, tif
    return step


def cancel(oid: str, now: int) -> dict[str, Any]:
    return {"op": "cancel", "id": oid, "now": now}


def advance_ts(ts: int) -> dict[str, Any]:
    return {"op": "advance_ts", "ts": ts}


def session_close(ts: int) -> dict[str, Any]:
    return {"op": "session_close", "ts": ts}


def set_position(qty: float, sym: str = "AAA") -> dict[str, Any]:
    return {"op": "set_position", "symbol": sym, "qty": qty}


DRAIN = {"op": "drain"}


# ---- expectation builders ------------------------------------------------------------------


def fill(oid: str, side: str, qty: float, price: float, ts: int, costs: int = 0, sym: str = "AAA"):
    return [oid, sym, side, qty, price, ts, costs]


def rej(oid: str, side: str, qty: float, reason: str, ts: int, sym: str = "AAA"):
    return [oid, sym, side, qty, reason, ts]


def drained(fills: list[Any] | None = None, rejections: list[Any] | None = None):
    return {"fills": fills or [], "rejections": rejections or []}


def cfg(cash: int = 1_000_000, **kw: Any) -> dict[str, Any]:
    return {"cash": cash, "currency": "INR", "long_only": True, "lot_sizes": {}, **kw}


def final(cash: int, fees: int, notional: int, positions: dict[str, float], working: list[str]):
    return {
        "cash": cash, "positions": positions, "working": working,
        "fees": fees, "traded_notional": notional,
    }  # fmt: skip


def scenario(name: str, config: dict[str, Any], steps: list[Any], drains: list[Any], fin: Any):
    return {
        "name": name, "chunk": 1, "config": config, "steps": steps,
        "expect": {"drains": drains, "final": fin},
    }  # fmt: skip


# ---- scenarios (expectations are hand-derived; comments show the arithmetic) ----------------
# Money: notional_minor = qty * price * 100, starting cash 1_000_000 minor (Rs 10,000).


def scenarios() -> list[dict[str, Any]]:
    out = []

    # Market: buys lift the ask, sells hit the bid. Buy 10 @101 = 101_000; sell 5 @99 = 49_500.
    # cash 1_000_000 - 101_000 + 49_500 = 948_500; position 5 + 10 - 5 = 10.
    out.append(scenario(
        "market_fills_at_the_opposite_touch", cfg(),
        [set_position(5), quote(1, 99.0, 101.0, 100, 100),
         submit("B", "buy", "market", 10, 2), submit("S", "sell", "market", 5, 2), DRAIN],
        [drained([fill("B", "buy", 10, 101.0, 2), fill("S", "sell", 5, 99.0, 2)])],
        final(948_500, 0, 150_500, {"AAA": 10}, []),
    ))  # fmt: skip

    # Market, partial by top size: ask size 4 -> 4 @101 now; the 6 left works and takes the
    # next quote's ask (102, size 10). 40_400 + 61_200 = 101_600.
    out.append(scenario(
        "market_partial_by_top_size_then_rest_on_next_quote", cfg(),
        [quote(1, 99.0, 101.0, 10, 4), submit("M", "buy", "market", 10, 2), DRAIN,
         quote(3, 99.0, 102.0, 10, 10), DRAIN],
        [drained([fill("M", "buy", 4, 101.0, 2)]), drained([fill("M", "buy", 6, 102.0, 3)])],
        final(898_400, 0, 101_600, {"AAA": 10}, []),
    ))  # fmt: skip

    # Consumed touch size is gone until the next quote: ask size 5; A takes 5, B gets nothing
    # and waits; the next quote (size 10) gives B its 3. 50_500 + 30_300 = 80_800.
    out.append(scenario(
        "consumed_touch_size_is_not_reused", cfg(),
        [quote(1, 99.0, 101.0, 10, 5), submit("A", "buy", "market", 5, 2),
         submit("B", "buy", "market", 3, 2), DRAIN, quote(3, 99.0, 101.0, 10, 10), DRAIN],
        [drained([fill("A", "buy", 5, 101.0, 2)]), drained([fill("B", "buy", 3, 101.0, 3)])],
        final(919_200, 0, 80_800, {"AAA": 8}, []),
    ))  # fmt: skip

    # A market order with no book waits for the first quote and fills at that quote's time.
    out.append(scenario(
        "market_waits_for_the_first_quote", cfg(),
        [submit("M", "buy", "market", 5, 1), DRAIN, quote(2, 99.0, 101.0, 10, 10), DRAIN],
        [drained(), drained([fill("M", "buy", 5, 101.0, 2)])],
        final(949_500, 0, 50_500, {"AAA": 5}, []),
    ))  # fmt: skip

    # Marketable limit (buy 10 @ limit 102, ask 101 size 4): 4 @101 at the touch price, the 6
    # left rest at 102. A print at 102 (size 3) trades through: 3 @ the limit 102; a print at
    # 101 (size 10) fills the last 3 @102. 40_400 + 30_600 + 30_600 = 101_600.
    out.append(scenario(
        "marketable_limit_walks_touch_and_remainder_rests", cfg(),
        [quote(1, 99.0, 101.0, 10, 4), submit("L", "buy", "limit", 10, 2, price=102.0), DRAIN,
         trade(3, 102.0, 3), DRAIN, trade(4, 101.0, 10), DRAIN],
        [drained([fill("L", "buy", 4, 101.0, 2)]), drained([fill("L", "buy", 3, 102.0, 3)]),
         drained([fill("L", "buy", 3, 102.0, 4)])],
        final(898_400, 0, 101_600, {"AAA": 10}, []),
    ))  # fmt: skip

    # Resting limits fill at their limit when a print trades through. Book 99/101 so both rest.
    # Print 100.5 (size 10): sell limit 100 (price >= 100) fills 4; buy limit 100 does not
    # (100.5 > 100). Print 100 size 2: buy fills 2. Print 99 size 10: buy fills the last 3.
    # cash 1_000_000 + 40_000 - 20_000 - 30_000 = 990_000; position 10 - 4 + 5 = 11.
    out.append(scenario(
        "resting_limit_fills_on_trade_through", cfg(),
        [set_position(10), quote(1, 99.0, 101.0, 10, 10),
         submit("BL", "buy", "limit", 5, 2, price=100.0),
         submit("SL", "sell", "limit", 4, 2, price=100.0), DRAIN,
         trade(3, 100.5, 10), DRAIN, trade(4, 100.0, 2), DRAIN, trade(5, 99.0, 10), DRAIN],
        [drained(), drained([fill("SL", "sell", 4, 100.0, 3)]),
         drained([fill("BL", "buy", 2, 100.0, 4)]), drained([fill("BL", "buy", 3, 100.0, 5)])],
        final(990_000, 0, 90_000, {"AAA": 11}, []),
    ))  # fmt: skip

    # A resting limit that a later quote crosses fills at the new touch (99.5 <= 100):
    # 5 * 99.5 = 497.5 -> 49_750.
    out.append(scenario(
        "resting_limit_crossed_by_a_quote_fills_at_the_touch", cfg(),
        [quote(1, 99.0, 101.0, 10, 10), submit("L", "buy", "limit", 5, 2, price=100.0), DRAIN,
         quote(3, 98.0, 99.5, 10, 10), DRAIN],
        [drained(), drained([fill("L", "buy", 5, 99.5, 3)])],
        final(950_250, 0, 49_750, {"AAA": 5}, []),
    ))  # fmt: skip

    # Stop-market buy, trigger 105. A print at 106 BEFORE submission does not trigger it; a
    # print at 104 does not cross; 105 does, and it then fills as a market order at the ask 101.
    out.append(scenario(
        "stop_market_triggers_on_a_trade_then_fills_at_the_touch", cfg(),
        [trade(1, 106.0, 1), quote(2, 99.0, 101.0, 10, 10),
         submit("ST", "buy", "stop_market", 5, 3, trigger=105.0), trade(4, 104.0, 1), DRAIN,
         trade(5, 105.0, 1), DRAIN],
        [drained(), drained([fill("ST", "buy", 5, 101.0, 5)])],
        final(949_500, 0, 50_500, {"AAA": 5}, []),
    ))  # fmt: skip

    # Stop-limit sell, trigger 98, limit 97, bid 99: the print at 98 triggers it; as a limit it
    # is marketable (99 >= 97) and fills at the touch 99 (limit protects, no worse than 97).
    out.append(scenario(
        "stop_limit_sell_triggers_then_fills_at_the_better_touch", cfg(),
        [set_position(10), quote(1, 99.0, 101.0, 10, 10),
         submit("SL", "sell", "stop_limit", 10, 2, price=97.0, trigger=98.0),
         trade(3, 99.0, 1), DRAIN, trade(4, 98.0, 1), DRAIN],
        [drained(), drained([fill("SL", "sell", 10, 99.0, 4)])],
        final(1_099_000, 0, 99_000, {}, []),
    ))  # fmt: skip

    # Stop-limit buy, trigger 105, limit 103, ask 106 (not marketable, so it rests once armed).
    # The print at 105 triggers it but does not fill it; a later print at 103 (size 2) trades
    # through (2 @103), then 102 (size 10) fills the last 3 @103. 5 * 103 * 100 = 51_500.
    out.append(scenario(
        "stop_limit_buy_triggers_then_rests_as_a_limit", cfg(),
        [quote(1, 104.0, 106.0, 10, 10),
         submit("SL", "buy", "stop_limit", 5, 2, price=103.0, trigger=105.0),
         trade(3, 105.0, 1), DRAIN, trade(4, 103.0, 2), DRAIN, trade(5, 102.0, 10), DRAIN],
        [drained(), drained([fill("SL", "buy", 2, 103.0, 4)]),
         drained([fill("SL", "buy", 3, 103.0, 5)])],
        final(948_500, 0, 51_500, {"AAA": 5}, []),
    ))  # fmt: skip

    # IOC: limit 101 buy 10 vs ask 101 size 4 fills 4 and cancels the 6 left at once. A second
    # IOC limit 100 is not marketable (ask 101), so it cancels whole: 5.
    out.append(scenario(
        "ioc_fills_what_it_can_and_cancels_the_remainder", cfg(),
        [quote(1, 99.0, 101.0, 10, 4), submit("I", "buy", "limit", 10, 2, price=101.0, tif="ioc"),
         submit("J", "buy", "limit", 5, 3, price=100.0, tif="ioc"), DRAIN],
        [drained([fill("I", "buy", 4, 101.0, 2)],
                 [rej("I", "buy", 6, "cancelled", 2), rej("J", "buy", 5, "cancelled", 3)])],
        final(959_600, 0, 40_400, {"AAA": 4}, []),
    ))  # fmt: skip

    # FOK: 6 > ask size 5 is rejected whole (no fill, no change to the book); 5 fills fully.
    out.append(scenario(
        "fok_is_all_or_nothing_against_the_touch_size", cfg(),
        [quote(1, 99.0, 101.0, 10, 5), submit("F1", "buy", "market", 6, 2, tif="fok"),
         submit("F2", "buy", "market", 5, 3, tif="fok"), DRAIN],
        [drained([fill("F2", "buy", 5, 101.0, 3)], [rej("F1", "buy", 6, "fok_unfillable", 2)])],
        final(949_500, 0, 50_500, {"AAA": 5}, []),
    ))  # fmt: skip

    # Day expiry: the default close is 23:59:00 UTC (CLOSE). Day order D and GTC order G rest at
    # 100. A print at CLOSE-1 changes nothing. The print at CLOSE+1 (100, size 100): D expires
    # first, stamped CLOSE; G fills 5 @100 = 50_000. Then a new Day order E (placed after the
    # close) expires at the NEXT close, CLOSE + DAY, when time is advanced past it.
    out.append(scenario(
        "day_orders_expire_at_the_session_close_gtc_do_not", cfg(),
        [quote(1_000_000_000, 99.0, 101.0, 10, 10),
         submit("D", "buy", "limit", 5, 1_000_000_001, price=100.0),
         submit("G", "buy", "limit", 5, 1_000_000_001, price=100.0, tif="gtc"),
         trade(CLOSE - 1, 200.0, 1), DRAIN, trade(CLOSE + 1, 100.0, 100), DRAIN,
         submit("E", "buy", "limit", 5, CLOSE + 10, price=100.0), advance_ts(CLOSE + DAY - 1),
         DRAIN, advance_ts(CLOSE + DAY + 5), DRAIN],
        [drained(), drained([fill("G", "buy", 5, 100.0, CLOSE + 1)],
                            [rej("D", "buy", 5, "expired", CLOSE)]),
         drained(), drained([], [rej("E", "buy", 5, "expired", CLOSE + DAY)])],
        final(950_000, 0, 50_000, {"AAA": 5}, []),
    ))  # fmt: skip

    # An explicit session close expires every working Day order at the close instant.
    out.append(scenario(
        "explicit_session_close_expires_day_orders", cfg(),
        [quote(1, 99.0, 101.0, 10, 10), submit("D", "buy", "limit", 5, 2, price=100.0),
         submit("G", "buy", "limit", 5, 2, price=100.0, tif="gtc"), session_close(900), DRAIN],
        [drained([], [rej("D", "buy", 5, "expired", 900)])],
        final(1_000_000, 0, 0, {}, ["G"]),
    ))  # fmt: skip

    # FIFO at one price: A, B, C each buy 5 @100 (submitted in that order). One print of size 7
    # fills A fully (5) and B partly (2) - the print's size is shared in queue order, C waits.
    # A print of size 20 then fills B's 3 and C's 5. 15 * 100 * 100 = 150_000.
    out.append(scenario(
        "fifo_priority_and_shared_trade_size", cfg(),
        [quote(1, 99.0, 101.0, 10, 10),
         submit("A", "buy", "limit", 5, 2, price=100.0),
         submit("B", "buy", "limit", 5, 3, price=100.0),
         submit("C", "buy", "limit", 5, 4, price=100.0),
         trade(5, 100.0, 7), DRAIN, trade(6, 100.0, 20), DRAIN],
        [drained([fill("A", "buy", 5, 100.0, 5), fill("B", "buy", 2, 100.0, 5)]),
         drained([fill("B", "buy", 3, 100.0, 6), fill("C", "buy", 5, 100.0, 6)])],
        final(850_000, 0, 150_000, {"AAA": 15}, []),
    ))  # fmt: skip

    # Cancel timing (ADR 0019 decision 6): the cancellation is stamped `now` (7, not the order's
    # or the last event's time) and the order no longer fills on a later print. P is cancelled
    # after a partial fill (2 of 6), reporting the 4 left. Cancelling an unknown id is a no-op.
    out.append(scenario(
        "cancel_is_stamped_at_the_cancel_time", cfg(),
        [quote(1, 99.0, 101.0, 10, 10), submit("L", "buy", "limit", 5, 2, price=100.0),
         cancel("L", 7), cancel("ZZ", 7), trade(8, 100.0, 5), DRAIN,
         submit("P", "buy", "limit", 6, 9, price=100.0), trade(10, 100.0, 2),
         cancel("P", 12), trade(13, 100.0, 5), DRAIN],
        [drained([], [rej("L", "buy", 5, "cancelled", 7)]),
         drained([fill("P", "buy", 2, 100.0, 10)], [rej("P", "buy", 4, "cancelled", 12)])],
        final(980_000, 0, 20_000, {"AAA": 2}, []),
    ))  # fmt: skip

    # Trailing stops are not supported (as in the L1 simulator): rejected on submit.
    out.append(scenario(
        "trailing_stop_is_rejected", cfg(),
        [submit("T", "sell", "trailing_stop", 1, 1), DRAIN],
        [drained([], [rej("T", "sell", 1, "unsupported_order_type", 1)])],
        final(1_000_000, 0, 0, {}, []),
    ))  # fmt: skip

    # Insufficient funds with costs (1% on buys). Cash 50_500 minor, ask 100, size 100. Buying
    # 10 costs 100_000 + 1_000; the largest affordable lot count is 5: 50_000 + 500 = 50_500
    # (6 would need 60_000 + 600). The 5 fill with cost 500, the other 5 are rejected.
    out.append(scenario(
        "insufficient_funds_cuts_the_fill_and_rejects_the_rest",
        cfg(50_500, costs={"bps_buy": 100}),
        [quote(1, 99.0, 100.0, 10, 100), submit("B", "buy", "market", 10, 2), DRAIN],
        [drained([fill("B", "buy", 5, 100.0, 2, 500)],
                 [rej("B", "buy", 5, "insufficient_funds", 2)])],
        final(0, 500, 50_000, {"AAA": 5}, []),
    ))  # fmt: skip

    # Long-only sell cap. Holding 3, selling 5 fills 3 @99 and rejects the 2 with no_position;
    # a further sell of 1 with nothing held is rejected whole. Cash + 3 * 99 * 100 = 29_700.
    out.append(scenario(
        "sell_beyond_the_position_is_cut_with_no_position", cfg(),
        [set_position(3), quote(1, 99.0, 101.0, 10, 10), submit("S1", "sell", "market", 5, 2),
         submit("S2", "sell", "market", 1, 3), DRAIN],
        [drained([fill("S1", "sell", 3, 99.0, 2)],
                 [rej("S1", "sell", 2, "no_position", 2), rej("S2", "sell", 1, "no_position", 3)])],
        final(1_029_700, 0, 29_700, {}, []),
    ))  # fmt: skip

    # Determinism: the same event sequence on two symbols (offset in time) yields mirrored
    # results, and the replay runs every scenario twice and demands identical output. Per
    # symbol: buy limit 5 @100 rests; print 100 x3 fills 3; print 99 x10 fills 2; market buy 4
    # lifts the ask 101. 30_000 + 20_000 + 40_400 = 90_400 per symbol, 180_800 in total.
    steps: list[Any] = []
    fills_a: list[Any] = []
    for sym, t0 in (("AAA", 10), ("BBB", 20)):
        steps += [
            quote(t0, 99.0, 101.0, 10, 10, sym),
            submit(f"L-{sym}", "buy", "limit", 5, t0 + 1, sym, price=100.0),
            trade(t0 + 2, 100.0, 3, sym), trade(t0 + 3, 99.0, 10, sym),
            submit(f"M-{sym}", "buy", "market", 4, t0 + 4, sym),
        ]  # fmt: skip
        fills_a += [
            fill(f"L-{sym}", "buy", 3, 100.0, t0 + 2, 0, sym),
            fill(f"L-{sym}", "buy", 2, 100.0, t0 + 3, 0, sym),
            fill(f"M-{sym}", "buy", 4, 101.0, t0 + 4, 0, sym),
        ]
    out.append(scenario(
        "deterministic_replay_across_symbols", cfg(),
        steps + [DRAIN], [drained(fills_a)],
        final(819_200, 0, 180_800, {"AAA": 9, "BBB": 9}, []),
    ))  # fmt: skip

    return out


# ---- document ------------------------------------------------------------------------------


def build() -> dict[str, Any]:
    return {
        "fixture_version": FIXTURE_VERSION,
        "type": "L2Sim",
        "description": (
            "L2 top-of-book simulator vectors (ADR 0030). Expectations are hand-derived from the "
            "ADR 0030 section 3 matching rules, not recorded from an implementation. See "
            "scripts/gen_l2_vectors.py for the step and expectation schema. `chunk` says which "
            "implementation chunk covers the scenario; a runner replays only the chunks it "
            "implements."
        ),
        "scenarios": scenarios(),
    }


def render(doc: dict[str, Any]) -> str:
    """One scenario header per block, one step per line: reviewable diffs, stable bytes."""
    lines = [
        "{",
        f'  "fixture_version": {doc["fixture_version"]},',
        f'  "type": {json.dumps(doc["type"])},',
        f'  "description": {json.dumps(doc["description"])},',
        '  "scenarios": [',
    ]
    blocks = []
    for sc in doc["scenarios"]:
        steps = ",\n".join(f"        {json.dumps(s)}" for s in sc["steps"])
        drains = ",\n".join(f"        {json.dumps(d)}" for d in sc["expect"]["drains"])
        blocks.append(
            "    {\n"
            f'      "name": {json.dumps(sc["name"])},\n'
            f'      "chunk": {sc["chunk"]},\n'
            f'      "config": {json.dumps(sc["config"])},\n'
            f'      "steps": [\n{steps}\n      ],\n'
            f'      "expect": {{\n        "drains": [\n{drains}\n        ],\n'
            f'        "final": {json.dumps(sc["expect"]["final"])}\n      }}\n'
            "    }"
        )
    lines.append(",\n".join(blocks))
    lines += ["  ]", "}", ""]
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    text = render(build())
    json.loads(text)  # the hand-rendered text must be valid JSON
    if "--check" in argv:
        return 0 if OUT.read_text(encoding="utf-8") == text else 1
    OUT.write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
