"""Regenerates ``schema/conformance/next_open_sim.json`` from the PYTHON ``NextOpenExecution``.

The Python simulator (``honba.backtest.simulated``) is the reference (ADR 0016): every expected
fill, rejection, cash and position below is computed by it, once, and committed. The Rust
``honba_sim::NextOpenSim`` (``crates/honba-sim/tests/next_open_conformance.rs``) and the Python
conformance test (``python/tests/integration/test_next_open_sim_conformance.py``) replay the file;
the Python test imports :func:`replay` from here, so the generator and the test cannot drift.

Run from ``python/``:  ``python ../scripts/gen_next_open_vectors.py``  (``--check`` only compares).
Deterministic: no wall clock, no randomness.

Scenario schema (``fixture_version`` 1)::

    {"name", "chunk", "config": {"cash", "currency", "long_only", "settlement_days", "lot_sizes",
                                 "costs"?},
     "steps": [...], "expect": {...}}

``config.costs`` (chunk 2, optional) is a test cost model both runners implement identically:
``{"flat_buy", "flat_sell", "bps_buy", "bps_sell"}`` (all optional, integers; flats in minor
units, may be negative to provoke the error path). The cost of a fill is
``flat + (notional_minor * bps + 5000) // 10000`` where ``notional_minor`` is the fill's
``mul_qty`` notional. (The real India schedule is not replayed here: it lives above ``honba-sim``,
ADR 0016 chunk 2 addendum.)

``cash`` is integer minor units. Steps, replayed in order (symbols are NSE instruments, prices and
quantities are plain numbers):

* ``{"op": "bar", "symbol", "ts", "open", "high", "low", "close"}``: feed one bar event.
* ``{"op": "open_session", "ts", "bars": [bar, ...]}``: open a session with all its bars.
* ``{"op": "submit", "id", "symbol", "side": "buy"|"sell", "type": "market"|"limit"|
  "stop_market"|"stop_limit", "qty", "ts", "price"?, "trigger"?}``
* ``{"op": "cancel", "id", "now"}``: ``now`` is the engine time of the cancel; the Python
  backends and the Rust binding stamp the ``Cancelled`` event with it (ADR 0019).
* ``{"op": "drain"}``: drain fills and rejections; recorded in ``expect.drains``.
* ``{"op": "session_open", "ts", "bars": [bar, ...]}`` (chunk 2): the ``SessionOpen`` event.
* ``{"op": "set_settlement_days", "days"}`` (chunk 2): ``set_settlement_days``.
* ``{"op": "probe"}`` (chunk 2): records ``{"unsettled", "available_cash"}`` (minor units) in
  ``expect.final.probes``.

A step that raises in the reference gets ``"error": true`` (the exception type is not compared).
``expect``: ``drains`` (list of ``{"fills", "rejections"}``) and ``final`` with ``cash``,
``positions`` (symbol -> quantity, sorted), ``working`` (order ids), ``fees``,
``traded_notional`` (minor units); chunk 2 scenarios add ``unsettled`` and ``available_cash`` (minor
units) and, when they probe, ``probes``. A fill is
``[order_id, symbol, side, quantity, price, ts, costs]`` and a rejection
``[order_id, symbol, side, quantity, reason, ts]``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from honba.backtest.simulated import FillCostFn, NextOpenExecution, SessionOpen
from honba.domain.money import Currency, Money
from honba.entities.bar import Bar
from honba.entities.instrument import InstrumentId
from honba.entities.order import OrderIntent, OrderSide, OrderType

OUT = Path(__file__).resolve().parents[1] / "schema" / "conformance" / "next_open_sim.json"
FIXTURE_VERSION = 1


def _iid(symbol: str) -> InstrumentId:
    return InstrumentId(symbol, "NSE")


def _bar(step: dict[str, Any]) -> Bar:
    return Bar(
        _iid(step["symbol"]),
        step["ts"],
        step["open"],
        step["high"],
        step["low"],
        step["close"],
        step.get("volume", 1000.0),
    )


def _intent(step: dict[str, Any]) -> OrderIntent:
    iid, qty = _iid(step["symbol"]), step["qty"]
    side = OrderSide.BUY if step["side"] == "buy" else OrderSide.SELL
    kind = step["type"]
    if kind == "market":
        return OrderIntent(iid, side, qty)
    if kind == "limit":
        return OrderIntent(iid, side, qty, OrderType.LIMIT, price=step["price"])
    if kind == "stop_market":
        return OrderIntent(iid, side, qty, OrderType.STOP_MARKET, trigger_price=step["trigger"])
    return OrderIntent(
        iid,
        side,
        qty,
        OrderType.STOP_LIMIT,
        price=step["price"],
        trigger_price=step["trigger"],
    )


def _side(side: OrderSide) -> str:
    return "buy" if side is OrderSide.BUY else "sell"


def _cost_fn(spec: dict[str, int], currency: Currency) -> FillCostFn:
    def cost(side: OrderSide, quantity: float, price: float) -> Money:
        tag = "buy" if side is OrderSide.BUY else "sell"
        notional = Money.mul_qty(quantity, price, currency).amount
        minor = spec.get(f"flat_{tag}", 0) + (notional * spec.get(f"bps_{tag}", 0) + 5000) // 10000
        return Money.from_minor(minor, currency)

    return cost


def _port(config: dict[str, Any], backend: str | None = None) -> NextOpenExecution:
    currency = Currency[config["currency"]]
    cash = Money.from_minor(config["cash"], currency)
    extra: dict[str, Any] = {}
    if "costs" in config:
        extra["costs"] = _cost_fn(config["costs"], currency)
    lots = {_iid(s): lot for s, lot in config.get("lot_sizes", {}).items()}
    if backend is not None:  # None: the class default (HONBA_SIM_BACKEND, else auto)
        extra["backend"] = backend
    return NextOpenExecution(
        cash=cash,
        settlement_days=config.get("settlement_days", 0),
        long_only=config.get("long_only", True),
        lot_sizes=lots,
        **extra,
    )


def replay(scenario: dict[str, Any], backend: str | None = None) -> dict[str, Any]:
    """Runs ``scenario`` through ``NextOpenExecution``; returns its steps (errors flagged) + expect.

    ``backend`` is ``"python"`` (the reference, what ``build`` uses to author the vectors),
    ``"native"`` or ``"auto"``; ``None`` leaves the choice to the class (ADR 0016, chunk 3b).
    """
    port = _port(scenario["config"], backend)
    steps: list[dict[str, Any]] = []
    drains: list[dict[str, Any]] = []
    probes: list[dict[str, Any]] = []
    for raw in scenario["steps"]:
        step = {k: v for k, v in raw.items() if k != "error"}
        try:
            op = step["op"]
            if op == "bar":
                port.on_event(_bar(step), step["ts"])
            elif op == "open_session":
                port.open_session(step["ts"], [_bar(b) for b in step["bars"]])
            elif op == "session_open":
                bars = tuple(_bar(b) for b in step["bars"])
                port.on_event(SessionOpen(step["ts"], bars), step["ts"])
            elif op == "set_settlement_days":
                port.set_settlement_days(step["days"])
            elif op == "probe":
                probes.append(
                    {
                        "unsettled": port.unsettled.amount,
                        "available_cash": port.available_cash.amount,
                    }
                )
            elif op == "submit":
                port.submit(step["id"], _intent(step), step["ts"])
            elif op == "cancel":
                port.cancel(step["id"], step["now"])
            elif op == "drain":
                drains.append(
                    {
                        "fills": [
                            [
                                f.order_id,
                                f.instrument_id.symbol,
                                _side(f.side),
                                f.quantity,
                                f.price,
                                f.ts,
                                f.costs.amount,
                            ]
                            for f in port.drain_fills()
                        ],
                        "rejections": [
                            [
                                r.order_id,
                                r.intent.instrument_id.symbol,
                                _side(r.intent.side),
                                r.intent.quantity,
                                r.reason,
                                r.ts,
                            ]
                            for r in port.drain_rejections()
                        ],
                    }
                )
            else:
                raise AssertionError(f"unknown op {op!r}")
        except (ValueError, RuntimeError):
            step["error"] = True
        steps.append(step)
    expect = {
        "drains": drains,
        "cash": port.cash.amount,
        "positions": {i.symbol: q for i, q in sorted(port.positions.items(), key=_pos_key)},
        "working": port.working_orders,
        "fees": port.fees.amount,
        "traded_notional": port.traded_notional.amount,
    }
    if scenario["chunk"] >= 2:
        expect["unsettled"] = port.unsettled.amount
        expect["available_cash"] = port.available_cash.amount
        if probes:
            expect["probes"] = probes
    return {"steps": steps, "expect": expect}


def _pos_key(item: tuple[InstrumentId, float]) -> tuple[str, str]:
    return (item[0].symbol, item[0].exchange)


# -- scenario authoring ------------------------------------------------------------------------
def b(symbol: str, ts: int, open_: float, close: float | None = None) -> dict[str, Any]:
    c = open_ if close is None else close
    return {
        "op": "bar",
        "symbol": symbol,
        "ts": ts,
        "open": open_,
        "high": max(open_, c),
        "low": min(open_, c),
        "close": c,
    }


def sess(ts: int, *bars: dict[str, Any]) -> dict[str, Any]:
    return {"op": "open_session", "ts": ts, "bars": [{**x, "op": "bar"} for x in bars]}


def order(
    oid: str,
    side: str,
    symbol: str,
    qty: float,
    ts: int,
    kind: str = "market",
    **extra: float,
) -> dict[str, Any]:
    return {
        "op": "submit",
        "id": oid,
        "symbol": symbol,
        "side": side,
        "type": kind,
        "qty": qty,
        "ts": ts,
        **extra,
    }


def cancel(oid: str, now: int) -> dict[str, Any]:
    return {"op": "cancel", "id": oid, "now": now}


DRAIN: dict[str, Any] = {"op": "drain"}
PROBE: dict[str, Any] = {"op": "probe"}


def sopen(ts: int, *bars: dict[str, Any]) -> dict[str, Any]:
    return {"op": "session_open", "ts": ts, "bars": [{**x, "op": "bar"} for x in bars]}


def set_days(days: int) -> dict[str, Any]:
    return {"op": "set_settlement_days", "days": days}


def cfg(cash_rupees: float, **kw: Any) -> dict[str, Any]:
    return {
        "cash": round(cash_rupees * 100),
        "currency": "INR",
        "long_only": True,
        "settlement_days": 0,
        "lot_sizes": {},
        **kw,
    }


def scenario(name: str, config: dict[str, Any], steps: list[dict[str, Any]], chunk: int = 1):
    return {"name": name, "chunk": chunk, "config": config, "steps": steps}


def scenario2(name: str, config: dict[str, Any], steps: list[dict[str, Any]]):
    return scenario(name, config, steps, chunk=2)


def settlement_steps(extra: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Buy 10 A (cash 1000 -> 0), sell them, and queue a buy of 10 B that only the sale funds."""

    def ab(ts: int) -> dict[str, Any]:
        return sess(ts, b("A", ts, 100.0), b("B", ts, 100.0))

    steps = [
        ab(1),
        order("buy-a", "buy", "A", 10, 1),
        ab(2),
        order("sell-a", "sell", "A", 10, 2),
        order("buy-b", "buy", "B", 10, 2),
        PROBE,
    ]
    for ts in range(3, 7):
        steps += [ab(ts), PROBE, DRAIN]
    return steps + (extra or [])


def costed(**spec: int) -> dict[str, Any]:
    return {"costs": spec}


def scenarios() -> list[dict[str, Any]]:
    return [
        scenario(
            "fills_at_next_open_not_decision_close",
            cfg(10_000),
            [
                b("AAA", 1, 100.0, 105.0),
                order("o-0", "buy", "AAA", 10, 1),
                DRAIN,
                b("AAA", 2, 110.0, 120.0),
                DRAIN,
            ],
        ),
        scenario(
            "order_waits_for_its_instrument_to_print",
            cfg(10_000),
            [
                b("AAA", 1, 100.0),
                order("o-0", "buy", "BBB", 1, 1),
                b("AAA", 2, 100.0),
                DRAIN,
                b("BBB", 3, 50.0),
                DRAIN,
            ],
        ),
        scenario(
            "same_session_bar_does_not_fill_an_order_submitted_in_it",
            cfg(10_000),
            [
                b("AAA", 1, 100.0),
                order("o-0", "buy", "BBB", 1, 1),
                b("BBB", 1, 50.0),
                DRAIN,
                b("BBB", 2, 60.0),
                DRAIN,
            ],
        ),
        scenario(
            "order_submitted_before_any_session_fills_at_the_first_open",
            cfg(10_000),
            [order("o-0", "buy", "AAA", 2, 0), b("AAA", 1, 100.0), DRAIN],
        ),
        scenario(
            "two_bars_one_ts_open_their_own_instruments",
            cfg(10_000),
            [
                b("AAA", 1, 100.0),
                order("o-0", "buy", "AAA", 1, 1),
                order("o-1", "buy", "BBB", 1, 1),
                b("AAA", 2, 101.0),
                b("BBB", 2, 51.0),
                DRAIN,
            ],
        ),
        scenario(
            "sells_fill_before_buys_in_a_session",
            cfg(1_000),
            [
                sess(1, b("AAA", 1, 100.0), b("BBB", 1, 100.0)),
                order("o-0", "buy", "AAA", 10, 1),
                sess(2, b("AAA", 2, 100.0), b("BBB", 2, 100.0)),
                DRAIN,
                order("o-1", "buy", "BBB", 10, 2),
                order("o-2", "sell", "AAA", 10, 2),
                sess(3, b("AAA", 3, 100.0), b("BBB", 3, 100.0)),
                DRAIN,
            ],
        ),
        scenario(
            "buys_fill_in_submission_order_until_cash_runs_out",
            cfg(1_000),
            [
                b("AAA", 1, 10.0),
                order("o-0", "buy", "AAA", 60, 1),
                order("o-1", "buy", "AAA", 60, 1),
                b("AAA", 2, 10.0),
                DRAIN,
            ],
        ),
        scenario(
            "sell_is_capped_at_the_position",
            cfg(10_000),
            [
                b("AAA", 1, 100.0),
                order("o-0", "buy", "AAA", 5, 1),
                b("AAA", 2, 100.0),
                order("o-1", "sell", "AAA", 8, 2),
                b("AAA", 3, 100.0),
                DRAIN,
            ],
        ),
        scenario(
            "sell_without_a_position_is_rejected",
            cfg(10_000),
            [
                b("AAA", 1, 100.0),
                order("o-0", "sell", "AAA", 3, 1),
                b("AAA", 2, 100.0),
                DRAIN,
            ],
        ),
        scenario(
            "sell_a_hair_over_the_position_is_float_residue",
            cfg(10_000),
            [
                b("AAA", 1, 100.0),
                order("o-0", "buy", "AAA", 0.3, 1),
                b("AAA", 2, 100.0),
                order("o-1", "sell", "AAA", 0.3 + 5e-10, 2),
                b("AAA", 3, 100.0),
                DRAIN,
            ],
        ),
        scenario(
            "short_sell_when_not_long_only",
            cfg(10_000, long_only=False),
            [
                b("AAA", 1, 100.0),
                order("o-0", "sell", "AAA", 4, 1),
                b("AAA", 2, 100.0),
                DRAIN,
            ],
        ),
        scenario(
            "buy_is_cut_to_whole_units_the_cash_allows",
            cfg(1_050),
            [
                b("AAA", 1, 100.0),
                order("o-0", "buy", "AAA", 20, 1),
                b("AAA", 2, 100.0),
                DRAIN,
            ],
        ),
        scenario(
            "buy_is_cut_to_whole_lots",
            cfg(805, lot_sizes={"AAA": 3}),
            [
                b("AAA", 1, 10.0),
                order("o-0", "buy", "AAA", 90, 1),
                b("AAA", 2, 10.0),
                DRAIN,
            ],
        ),
        scenario(
            "buy_with_no_cash_for_one_unit_is_rejected_whole",
            cfg(50),
            [
                b("AAA", 1, 100.0),
                order("o-0", "buy", "AAA", 2, 1),
                b("AAA", 2, 100.0),
                DRAIN,
            ],
        ),
        scenario(
            "fractional_quantity_notional_rounds_half_away_from_zero",
            cfg(1_000),
            [
                b("AAA", 1, 33.33),
                order("o-0", "buy", "AAA", 0.5, 1),
                order("o-1", "buy", "AAA", 1.5, 1),
                b("AAA", 2, 33.33),
                DRAIN,
            ],
        ),
        scenario(
            "unusable_open_means_the_instrument_did_not_print",
            cfg(10_000),
            [
                b("AAA", 1, 100.0),
                order("o-0", "buy", "AAA", 1, 1),
                b("AAA", 2, 0.0),
                DRAIN,
                b("AAA", 3, 90.0),
                DRAIN,
            ],
        ),
        scenario(
            "limit_and_stop_orders_are_unsupported",
            cfg(10_000),
            [
                b("AAA", 1, 100.0),
                order("o-0", "buy", "AAA", 1, 1, "limit", price=99.0),
                order("o-1", "sell", "AAA", 1, 1, "stop_market", trigger=95.0),
                order("o-2", "buy", "AAA", 1, 1, "stop_limit", price=101.0, trigger=100.0),
                DRAIN,
                b("AAA", 2, 100.0),
                DRAIN,
            ],
        ),
        scenario(
            "cancel_a_working_order",
            cfg(10_000),
            [
                b("AAA", 1, 100.0),
                order("o-0", "buy", "AAA", 3, 1),
                cancel("o-0", 1),
                b("AAA", 2, 100.0),
                DRAIN,
            ],
        ),
        scenario(
            "late_cancel_is_stamped_with_the_cancel_time",
            cfg(10_000),
            [
                b("AAA", 2, 100.0),
                order("o-0", "buy", "BBB", 3, 2),
                b("AAA", 5, 100.0),
                cancel("o-0", 5),
                DRAIN,
            ],
        ),
        scenario(
            "cancel_of_unknown_filled_or_cancelled_orders_is_a_no_op",
            cfg(10_000),
            [
                b("AAA", 1, 100.0),
                order("o-0", "buy", "AAA", 1, 1),
                order("o-1", "buy", "BBB", 1, 1),
                b("AAA", 2, 100.0),
                cancel("o-0", 2),
                cancel("nope", 2),
                cancel("o-1", 2),
                cancel("o-1", 2),
                DRAIN,
            ],
        ),
        scenario(
            "cancel_before_any_session_is_stamped_zero",
            cfg(10_000),
            [order("o-0", "buy", "AAA", 1, 0), cancel("o-0", 0), DRAIN],
        ),
        scenario(
            "a_cut_order_has_nothing_left_to_cancel",
            cfg(1_050),
            [
                b("AAA", 1, 100.0),
                order("o-0", "buy", "AAA", 20, 1),
                b("AAA", 2, 100.0),
                cancel("o-0", 2),
                DRAIN,
            ],
        ),
        scenario(
            "drain_twice_repeats_nothing",
            cfg(10_000),
            [
                b("AAA", 1, 100.0),
                order("o-0", "buy", "AAA", 1, 1),
                order("o-1", "buy", "BBB", 1, 1),
                cancel("o-1", 1),
                b("AAA", 2, 100.0),
                DRAIN,
                DRAIN,
            ],
        ),
        scenario(
            "duplicate_working_order_id_is_an_error",
            cfg(10_000),
            [
                b("AAA", 1, 100.0),
                order("o-0", "buy", "AAA", 1, 1),
                order("o-0", "buy", "AAA", 2, 1),
                b("AAA", 2, 100.0),
                DRAIN,
            ],
        ),
        scenario(
            "a_finished_order_id_may_be_reused",
            cfg(10_000),
            [
                b("AAA", 1, 100.0),
                order("o-0", "buy", "AAA", 1, 1),
                b("AAA", 2, 100.0),
                order("o-0", "buy", "AAA", 2, 2),
                b("AAA", 3, 100.0),
                DRAIN,
            ],
        ),
        scenario(
            "bar_before_the_session_is_non_monotonic",
            cfg(10_000),
            [
                b("AAA", 5, 100.0),
                order("o-0", "buy", "AAA", 1, 5),
                b("AAA", 4, 100.0),
                b("AAA", 6, 100.0),
                DRAIN,
            ],
        ),
        scenario(
            "second_bar_for_an_instrument_in_a_session_is_an_error",
            cfg(10_000),
            [
                b("AAA", 1, 100.0),
                b("AAA", 1, 101.0),
                b("BBB", 1, 50.0),
                b("AAA", 2, 100.0),
                DRAIN,
            ],
        ),
        scenario(
            "open_session_must_advance",
            cfg(10_000),
            [
                sess(3, b("AAA", 3, 100.0)),
                sess(3, b("AAA", 3, 100.0)),
                sess(2, b("AAA", 2, 1.0)),
            ]
            + [order("o-0", "buy", "AAA", 1, 3), sess(4, b("AAA", 4, 100.0)), DRAIN],
        ),
        scenario(
            "session_key_need_not_be_the_bar_ts",
            cfg(10_000),
            [
                sess(10, b("AAA", 1, 100.0)),
                order("o-0", "buy", "AAA", 1, 1),
                sess(20, b("AAA", 2, 101.0)),
                DRAIN,
            ],
        ),
        # ---- chunk 2: settlement ----
        scenario2(
            "settlement_t0_proceeds_fund_the_same_session_buy",
            cfg(1_000, settlement_days=0),
            settlement_steps(),
        ),
        scenario2(
            "settlement_t1_waiting_buy_fills_once_proceeds_settle",
            cfg(1_000, settlement_days=1),
            settlement_steps(),
        ),
        scenario2(
            "settlement_t2_waiting_buy_fills_once_proceeds_settle",
            cfg(1_000, settlement_days=2),
            settlement_steps(),
        ),
        scenario2(
            "waiting_buy_is_cut_once_the_wait_is_over",
            cfg(1_000, settlement_days=1),
            [
                sess(1, b("A", 1, 100.0), b("B", 1, 100.0)),
                order("buy-a", "buy", "A", 10, 1),
                sess(2, b("A", 2, 100.0), b("B", 2, 100.0)),
                order("sell-a", "sell", "A", 10, 2),
                order("buy-b", "buy", "B", 25, 2),
                sess(3, b("A", 3, 100.0), b("B", 3, 100.0)),
                PROBE,
                sess(4, b("A", 4, 100.0), b("B", 4, 100.0)),
                PROBE,
                DRAIN,
            ],
        ),
        scenario2(
            "waiting_buy_whose_instrument_does_not_print_starts_waiting_when_it_does",
            cfg(1_000, settlement_days=2),
            [
                sess(1, b("A", 1, 100.0)),
                order("buy-a", "buy", "A", 10, 1),
                sess(2, b("A", 2, 100.0)),
                order("sell-a", "sell", "A", 10, 2),
                order("buy-b", "buy", "B", 5, 2),
                sess(3, b("A", 3, 100.0)),
                PROBE,
                sess(4, b("A", 4, 100.0), b("B", 4, 100.0)),
                PROBE,
                sess(5, b("A", 5, 100.0), b("B", 5, 100.0)),
                PROBE,
                DRAIN,
            ],
        ),
        scenario2(
            "sale_proceeds_pending_in_two_sessions_sum_up_and_settle_separately",
            cfg(2_000, settlement_days=2),
            [
                sess(1, b("A", 1, 100.0), b("B", 1, 50.0)),
                order("buy-a", "buy", "A", 5, 1),
                order("buy-b", "buy", "B", 10, 1),
                sess(2, b("A", 2, 100.0), b("B", 2, 50.0)),
                order("sell-a", "sell", "A", 5, 2),
                sess(3, b("A", 3, 100.0), b("B", 3, 50.0)),
                order("sell-b", "sell", "B", 10, 3),
                PROBE,
                sess(4, b("A", 4, 100.0), b("B", 4, 50.0)),
                PROBE,
                sess(5, b("A", 5, 100.0), b("B", 5, 50.0)),
                PROBE,
                sess(6, b("A", 6, 100.0), b("B", 6, 50.0)),
                PROBE,
                DRAIN,
            ],
        ),
        scenario2(
            "short_sell_proceeds_also_settle_later",
            cfg(100, long_only=False, settlement_days=1),
            [
                sess(1, b("A", 1, 100.0)),
                order("short", "sell", "A", 4, 1),
                sess(2, b("A", 2, 100.0)),
                PROBE,
                sess(3, b("A", 3, 100.0)),
                PROBE,
                DRAIN,
            ],
        ),
        scenario2(
            "set_settlement_days_only_before_the_first_session",
            cfg(1_000, settlement_days=0),
            [
                set_days(1),
                set_days(-1),
                sess(1, b("A", 1, 100.0)),
                set_days(2),
                order("buy-a", "buy", "A", 10, 1),
                sess(2, b("A", 2, 100.0)),
                order("sell-a", "sell", "A", 10, 2),
                sess(3, b("A", 3, 100.0)),
                PROBE,
                DRAIN,
            ],
        ),
        # ---- chunk 2: costs ----
        scenario2(
            "costs_are_paid_on_buys_and_taken_from_sell_proceeds",
            cfg(10_000, **costed(flat_buy=250, flat_sell=300, bps_buy=10, bps_sell=20)),
            [
                b("A", 1, 100.0),
                order("buy-a", "buy", "A", 10, 1),
                b("A", 2, 100.0),
                order("sell-a", "sell", "A", 10, 2),
                b("A", 3, 101.0),
                DRAIN,
            ],
        ),
        scenario2(
            "costs_reduce_pending_sale_proceeds",
            cfg(2_000, settlement_days=1, **costed(flat_sell=5_000, bps_sell=100)),
            [
                b("A", 1, 100.0),
                order("buy-a", "buy", "A", 10, 1),
                b("A", 2, 100.0),
                order("sell-a", "sell", "A", 10, 2),
                b("A", 3, 100.0),
                PROBE,
                b("A", 4, 100.0),
                PROBE,
                DRAIN,
            ],
        ),
        scenario2(
            "costs_cut_a_buy_to_what_notional_plus_cost_allows",
            cfg(1_000, **costed(bps_buy=100)),
            [
                b("A", 1, 100.0),
                order("buy-a", "buy", "A", 20, 1),
                b("A", 2, 100.0),
                DRAIN,
            ],
        ),
        scenario2(
            "a_flat_cost_alone_can_cost_one_more_unit",
            cfg(1_000, **costed(flat_buy=10_000)),
            [
                b("A", 1, 100.0),
                order("buy-a", "buy", "A", 10, 1),
                b("A", 2, 100.0),
                DRAIN,
            ],
        ),
        scenario2(
            "a_cost_cut_floors_to_whole_lots",
            cfg(1_000, lot_sizes={"A": 3}, **costed(bps_buy=250)),
            [
                b("A", 1, 10.0),
                order("buy-a", "buy", "A", 90, 1),
                b("A", 2, 10.0),
                DRAIN,
            ],
        ),
        scenario2(
            "a_cost_larger_than_cash_rejects_the_whole_buy",
            cfg(100, **costed(flat_buy=1_000_000)),
            [b("A", 1, 10.0), order("buy-a", "buy", "A", 1, 1), b("A", 2, 10.0), DRAIN],
        ),
        scenario2(
            "an_exact_fit_including_cost_fills_in_full",
            cfg(1_010, **costed(flat_buy=1_000)),
            [
                b("A", 1, 100.0),
                order("buy-a", "buy", "A", 10, 1),
                b("A", 2, 100.0),
                DRAIN,
            ],
        ),
        scenario2(
            "negative_buy_cost_errors_and_leaves_the_order_working",
            cfg(10_000, **costed(flat_buy=-1)),
            [
                b("A", 1, 100.0),
                order("buy-a", "buy", "A", 1, 1),
                b("A", 2, 100.0),
                DRAIN,
                b("A", 3, 100.0),
                DRAIN,
            ],
        ),
        scenario2(
            "negative_sell_cost_errors_and_leaves_the_position",
            cfg(10_000, **costed(flat_sell=-5)),
            [
                b("A", 1, 100.0),
                order("buy-a", "buy", "A", 2, 1),
                b("A", 2, 100.0),
                order("sell-a", "sell", "A", 2, 2),
                b("A", 3, 100.0),
                DRAIN,
            ],
        ),
        # ---- chunk 2: the SessionOpen event ----
        scenario2(
            "session_open_fills_sells_before_buys_like_open_session",
            cfg(1_000),
            [
                sopen(1, b("A", 1, 100.0), b("B", 1, 100.0)),
                order("buy-a", "buy", "A", 10, 1),
                sopen(2, b("A", 2, 100.0), b("B", 2, 100.0)),
                order("buy-b", "buy", "B", 10, 2),
                order("sell-a", "sell", "A", 10, 2),
                sopen(3, b("A", 3, 100.0), b("B", 3, 100.0)),
                DRAIN,
            ],
        ),
        scenario2(
            "session_open_ignores_later_earlier_and_repeated_bars",
            cfg(10_000),
            [
                sopen(10, b("A", 10, 100.0)),
                order("buy-a", "buy", "A", 1, 10),
                b("A", 20, 120.0),
                b("A", 5, 90.0),
                b("A", 10, 95.0),
                DRAIN,
                sopen(11, b("A", 11, 101.0)),
                DRAIN,
            ],
        ),
        scenario2(
            "a_bar_at_the_session_ts_opens_an_instrument_the_session_open_left_out",
            cfg(10_000),
            [
                sopen(1, b("A", 1, 100.0)),
                order("buy-b", "buy", "B", 2, 1),
                sopen(2, b("A", 2, 100.0)),
                b("A", 2, 100.0),
                b("B", 2, 7.0),
                b("B", 2, 8.0),
                DRAIN,
            ],
        ),
        scenario2(
            "session_open_key_need_not_be_a_bar_ts_and_bars_never_open_sessions",
            cfg(10_000),
            [
                sopen(10, b("A", 1, 100.0)),
                order("buy-a", "buy", "A", 1, 1),
                b("A", 2, 101.0),
                b("A", 1, 102.0),
                DRAIN,
                sopen(20, b("A", 2, 103.0)),
                DRAIN,
            ],
        ),
        scenario2(
            "session_open_must_advance",
            cfg(10_000),
            [
                sopen(3, b("A", 3, 100.0)),
                sopen(3, b("A", 3, 100.0)),
                sopen(2, b("A", 2, 1.0)),
                order("buy-a", "buy", "A", 1, 3),
                sopen(4, b("A", 4, 100.0)),
                DRAIN,
            ],
        ),
        scenario2(
            "session_open_after_plain_bars_switches_to_the_lenient_rules",
            cfg(10_000),
            [
                b("A", 1, 100.0),
                order("buy-a", "buy", "A", 1, 1),
                sopen(2, b("A", 2, 101.0)),
                order("buy-b", "buy", "B", 1, 2),
                b("B", 2, 50.0),
                b("B", 3, 51.0),
                DRAIN,
            ],
        ),
    ]


def build() -> dict[str, Any]:
    out = []
    for sc in scenarios():
        res = replay(sc, "python")
        out.append({**sc, "steps": res["steps"], "expect": res["expect"]})
    return {
        "fixture_version": FIXTURE_VERSION,
        "type": "NextOpenSim",
        "description": (
            "Next-open simulator vectors generated from honba.backtest.simulated.NextOpenExecution "
            "(ADR 0016). See scripts/gen_next_open_vectors.py for the step and expectation schema. "
            "`chunk` says which implementation chunk covers the scenario; a runner replays only "
            "the chunks it implements."
        ),
        "scenarios": out,
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
        rest = {k: v for k, v in sc["expect"].items() if k != "drains"}
        blocks.append(
            "    {\n"
            f'      "name": {json.dumps(sc["name"])},\n'
            f'      "chunk": {sc["chunk"]},\n'
            f'      "config": {json.dumps(sc["config"])},\n'
            f'      "steps": [\n{steps}\n      ],\n'
            f'      "expect": {{\n        "drains": [\n{drains}\n        ],\n'
            f'        "final": {json.dumps(rest)}\n      }}\n'
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
