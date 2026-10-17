"""Regenerates ``schema/conformance/screener_scan.json`` from the PYTHON screener evaluator.

The Python evaluator (``honba.screener.evaluator``) is the reference: every expected result and
metric below is computed by it, once, and committed. The Rust evaluator
(``honba-indicators::screener``) and the Python conformance test both consume the file.

Run from ``python/``:  ``python ../scripts/gen_screener_scan_vectors.py``.
Cases whose Python behaviour is an exception record ``{"error": "<ExceptionName>"}``.
Metrics that Python does not compute from bars (it returns ``None`` -> predicate False) are listed
under ``unsupported``; the Rust evaluator must reject those with ``unsupported_metric`` instead of
answering false.
"""

from __future__ import annotations

import json
from pathlib import Path

from honba.domain.instrument import InstrumentId
from honba.entities.bar import Bar
from honba.entities.screener import ScreenerFilterGroup, ScreenerFilterPredicate
from honba.screener.evaluator import (
    evaluate_group_on_bars,
    evaluate_predicate_on_bars,
    extract_metrics_from_bars,
)

OUT = Path(__file__).resolve().parents[1] / "schema" / "conformance" / "screener_scan.json"
DAY_NS = 86_400_000_000_000


def lcg_series(n: int, seed: int, start: float) -> dict[str, list[float]]:
    state, px = seed, start
    o, h, lo, c, v = [], [], [], [], []
    for _ in range(n):
        state = (state * 1103515245 + 12345) % 2**31
        step = ((state >> 8) % 401 - 200) / 100.0
        op = px
        px = round(max(1.0, px + step), 2)
        o.append(round(op, 2))
        h.append(round(max(op, px) + 0.35, 2))
        lo.append(round(min(op, px) - 0.35, 2))
        c.append(px)
        v.append(float(1000 + (state >> 4) % 9000))
    return {"open": o, "high": h, "low": lo, "close": c, "volume": v}


def ohlcv(close: list[float], vol: float = 1000.0) -> dict[str, list[float]]:
    return {
        "open": [round(x - 0.1, 2) for x in close],
        "high": [round(x + 0.5, 2) for x in close],
        "low": [round(x - 0.5, 2) for x in close],
        "close": close,
        "volume": [vol + i for i in range(len(close))],
    }


INPUTS = {
    "short3": ohlcv([10.5, 11.25, 10.75]),
    "single": ohlcv([42.0]),
    # Falls then bounces: SMA3 crosses above SMA5 on the last bar.
    "cross_up": ohlcv(
        [20, 19.5, 19, 18.5, 18, 17.5, 17, 16.5, 16, 15.5, 15, 14.5, 14, 13.5, 13, 12.5, 12, 18]
    ),
    # Rises then drops: SMA3 crosses below SMA5 on the last bar.
    "cross_down": ohlcv(
        [10, 10.5, 11, 11.5, 12, 12.5, 13, 13.5, 14, 14.5, 15, 15.5, 16, 16.5, 17, 17.5, 18, 11]
    ),
    "up20": ohlcv([round(100 + i * 0.7 + (i % 3) * 0.15, 2) for i in range(20)]),
    "down20": ohlcv([round(120 - i * 0.9 + (i % 4) * 0.2, 2) for i in range(20)]),
    "flat20": ohlcv([50.0] * 20),
    "walk260": lcg_series(260, 7, 100.0),
}


def bars_of(name: str) -> list[Bar]:
    s = INPUTS[name]
    iid = InstrumentId("X", "NSE")
    n = len(s["close"])
    return [
        Bar(
            instrument_id=iid,
            ts=i * DAY_NS,
            open=s["open"][i],
            high=s["high"][i],
            low=s["low"][i],
            close=s["close"][i],
            volume=s["volume"][i],
        )
        for i in range(n)
    ]


def pred(key: str, op: str, value, **extra) -> dict:
    return {"key": key, "op": op, "value": value, **extra}


def ref(key: str) -> dict:
    return {"key": key}


CASES: list[tuple[str, str, dict]] = [
    # (name, input, predicate)
    ("close_gt_number_true", "short3", pred("close", "gt", 10)),
    ("close_gt_number_false", "short3", pred("close", "gt", 10.75)),
    ("close_gte_equal", "short3", pred("close", "gte", 10.75)),
    ("close_lt_true", "short3", pred("close", "lt", 11)),
    ("close_lt_equal_false", "short3", pred("close", "lt", 10.75)),
    ("close_lte_equal", "short3", pred("close", "lte", 10.75)),
    ("close_eq_float", "short3", pred("close", "eq", 10.75)),
    ("close_eq_int_valued", "single", pred("close", "eq", 42)),
    ("close_neq_true", "short3", pred("close", "neq", 1)),
    ("close_neq_equal_false", "short3", pred("close", "neq", 10.75)),
    ("close_eq_string_false", "short3", pred("close", "eq", "10.75")),
    ("close_neq_string_true", "short3", pred("close", "neq", "10.75")),
    ("close_eq_null_false", "short3", pred("close", "eq", None)),
    ("close_eq_true_bool", "single", pred("close", "eq", True)),
    ("open_lt_close", "short3", pred("open", "lt", ref("close"))),
    ("high_gt_low_ref", "short3", pred("high", "gt", ref("low"))),
    ("low_lte_close_ref", "short3", pred("low", "lte", ref("close"))),
    ("volume_gte_number", "short3", pred("volume", "gte", 1002)),
    ("volume_lt_number", "short3", pred("VOLUME", "lt", 1002)),
    ("close_between_inclusive_low", "short3", pred("close", "between", [10.75, 11])),
    ("close_between_inclusive_high", "short3", pred("close", "between", [10, 10.75])),
    ("close_between_outside", "short3", pred("close", "between", [11, 12])),
    ("close_in_hit", "short3", pred("close", "in", [10.5, 10.75])),
    ("close_in_miss", "short3", pred("close", "in", [1, 2, "x"])),
    ("close_in_empty", "short3", pred("close", "in", [])),
    ("close_not_in_hit", "short3", pred("close", "not_in", [10.75])),
    ("close_not_in_miss", "short3", pred("close", "not_in", [1, 2])),
    ("close_in_int_valued", "single", pred("close", "in", [42, 43])),
    ("close_like_substring", "short3", pred("close", "like", "0.7")),
    ("close_like_miss", "short3", pred("close", "like", "9")),
    ("close_has_substring", "short3", pred("close", "has", "10.")),
    ("close_like_int_rhs", "single", pred("close", "like", 42)),
    ("close_like_float_repr_dot_zero", "single", pred("close", "like", "42.0")),
    ("close_like_case_insensitive", "single", pred("close", "like", "")),
    ("close_like_null_false", "short3", pred("close", "like", None)),
    ("close_gt_string_raises", "short3", pred("close", "gt", "5")),
    ("close_lte_string_raises", "short3", pred("close", "lte", "abc")),
    ("sma3_gt_close", "up20", pred("SMA3", "lt", ref("close"))),
    ("sma5_gt_number", "up20", pred("sma5", "gt", 105)),
    ("sma20_lt_number", "up20", pred("SMA20", "lt", 107)),
    ("sma5_warmup_false", "short3", pred("SMA5", "gt", 0)),
    ("sma3_between", "up20", pred("SMA3", "between", [100, 120])),
    ("sma10_eq_flat", "flat20", pred("SMA10", "eq", 50)),
    ("rsi_gt_up", "up20", pred("RSI", "gt", 70)),
    ("rsi_lt_down", "down20", pred("rsi", "lt", 30)),
    ("rsi_flat_is_100", "flat20", pred("RSI", "gte", 100)),
    ("rsi_warmup_false", "short3", pred("RSI", "gt", 0)),
    ("rsi_between", "walk260", pred("RSI", "between", [0, 100])),
    ("sma3_crosses_above_sma5", "cross_up", pred("SMA3", "crosses_above", ref("SMA5"))),
    ("sma3_crosses_below_sma5_no", "cross_up", pred("SMA3", "crosses_below", ref("SMA5"))),
    ("sma3_crosses_below_sma5", "cross_down", pred("SMA3", "crosses_below", ref("SMA5"))),
    ("sma3_crosses_above_sma5_no", "cross_down", pred("SMA3", "crosses_above", ref("SMA5"))),
    ("close_crosses_above_number", "cross_up", pred("close", "crosses_above", 17)),
    ("close_crosses_above_number_no", "up20", pred("close", "crosses_above", 105)),
    ("close_crosses_below_number", "cross_down", pred("close", "crosses_below", 12)),
    ("close_crosses_above_equal_prev", "short3", pred("close", "crosses_above", 11.25)),
    ("crosses_needs_two_bars", "single", pred("close", "crosses_above", 1)),
    ("crosses_ref_warmup", "short3", pred("SMA3", "crosses_above", ref("SMA5"))),
    ("high52_gte_close", "walk260", pred("price_52_week_high", "gte", ref("close"))),
    ("low52_lte_close", "walk260", pred("price_52_week_low", "lte", ref("close"))),
    ("low52_between", "walk260", pred("price_52_week_low", "between", [0, 1000])),
    ("high52_insufficient_bars", "up20", pred("price_52_week_high", "gt", 0)),
    ("low52_insufficient_bars", "up20", pred("price_52_week_low", "gt", 0)),
    ("close_ref_to_sma_scalar_side", "walk260", pred("close", "gt", ref("SMA50"))),
    ("close_gt_ref_unwarmed", "short3", pred("close", "gt", ref("SMA50"))),
]

UNSUPPORTED = [
    ("unknown_key", "short3", pred("not_a_metric", "gt", 1)),
    ("fundamental_pe_ttm", "short3", pred("price_earnings_ttm", "lt", 20, period="TTM")),
    ("fundamental_market_cap", "short3", pred("market_cap", "gte", 1000)),
    ("unsupported_ref_key", "short3", pred("close", "gt", ref("ema20"))),
    ("sma_zero_period", "up20", pred("sma0", "gt", 1)),
]

# Python quirk: the 252-bar invariant compares ``pred.key`` case-SENSITIVELY while the metric itself
# is matched case-insensitively, so ``PRICE_52_WEEK_LOW`` over 20 bars is computed from a short
# window and can be True. The Rust evaluator applies the invariant case-insensitively (the
# documented intent), so these cases pin BOTH answers and are reported as a discrepancy.
DIVERGENCES = [
    ("low52_upper_case_key_insufficient", "up20", pred("PRICE_52_WEEK_LOW", "gt", 0), False),
    ("high52_upper_case_key_insufficient", "up20", pred("Price_52_Week_High", "gt", 0), False),
]

GROUPS = [
    (
        "group_and_all_true",
        "up20",
        {"operator": "AND", "items": [pred("close", "gt", 100), pred("SMA3", "gt", 100)]},
    ),
    (
        "group_and_one_false",
        "up20",
        {"operator": "AND", "items": [pred("close", "gt", 100), pred("close", "lt", 100)]},
    ),
    (
        "group_or_one_true",
        "up20",
        {"operator": "OR", "items": [pred("close", "lt", 100), pred("RSI", "gt", 70)]},
    ),
    (
        "group_or_all_false",
        "up20",
        {"operator": "OR", "items": [pred("close", "lt", 100), pred("RSI", "lt", 10)]},
    ),
    ("group_empty_and", "up20", {"operator": "AND", "items": []}),
    ("group_empty_or", "up20", {"operator": "OR", "items": []}),
    (
        "group_nested",
        "up20",
        {
            "operator": "AND",
            "items": [
                pred("close", "gt", 100),
                {"operator": "OR", "items": [pred("RSI", "lt", 10), pred("SMA5", "gt", 100)]},
            ],
        },
    ),
]


def keys_of(p: dict) -> list[str]:
    keys = [p["key"]]
    v = p["value"]
    if isinstance(v, dict) and "key" in v and p["op"] in (
        "gt", "gte", "lt", "lte", "crosses_above", "crosses_below"
    ):
        keys.append(v["key"])
    return keys


def run_predicate(p: dict, bars: list[Bar]) -> dict:
    try:
        model = ScreenerFilterPredicate.model_validate(p)
        result = evaluate_predicate_on_bars(model, bars)
    except Exception as exc:  # noqa: BLE001 - recorded as the reference behaviour
        return {"error": type(exc).__name__}
    return {"result": result}


def main() -> None:
    cases = []
    for name, inp, p in CASES:
        bars = bars_of(inp)
        out = run_predicate(p, bars)
        metrics = extract_metrics_from_bars(keys_of(p), bars)
        case = {"name": name, "input": inp, "predicate": p, "expected": out}
        if "result" in out:
            case["expected"]["metrics"] = metrics
        cases.append(case)
    unsupported = []
    for name, inp, p in UNSUPPORTED:
        bars = bars_of(inp)
        out = run_predicate(p, bars)
        assert out == {"result": False} or "error" in out, (name, out)
        unsupported.append(
            {"name": name, "input": inp, "predicate": p, "python": out, "rust": "unsupported_metric"}
        )
    divergences = []
    for name, inp, p, rust in DIVERGENCES:
        out = run_predicate(p, bars_of(inp))
        assert out["result"] is not rust, (name, out)
        divergences.append(
            {"name": name, "input": inp, "predicate": p, "python": out, "rust": {"result": rust}}
        )
    groups = []
    for name, inp, g in GROUPS:
        bars = bars_of(inp)
        model = ScreenerFilterGroup.model_validate(g)
        groups.append(
            {
                "name": name,
                "input": inp,
                "group": g,
                "expected": {"result": evaluate_group_on_bars(model, bars)},
            }
        )
    results = [c["expected"].get("result") for c in cases]
    assert True in results and False in results and any("error" in c["expected"] for c in cases)
    doc = {
        "$comment": (
            "Generated once from the Python evaluator by scripts/gen_screener_scan_vectors.py."
            " Consumed by crates/honba-indicators/tests/screener_conformance.rs and"
            " python/tests/integration/test_screener_scan_conformance.py."
        ),
        "type": "ScreenerScan",
        "tolerance": {"relative": 1e-9},
        "inputs": INPUTS,
        "cases": cases,
        "groups": groups,
        "unsupported": unsupported,
        "divergences": divergences,
    }
    OUT.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {OUT}: {len(cases)} cases, {len(groups)} groups, {len(unsupported)} unsupported")


if __name__ == "__main__":
    main()
