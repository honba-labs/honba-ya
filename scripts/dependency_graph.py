"""Crate layering for the async/API surface added in docs/archive/plan.md phases 1-2.

Dependencies point inward, so L0 (honba-messages) may depend on nothing and a
binary at L7 may depend on most of the stack. Two rules are stricter than the
plain hierarchy:

- Async isolation. The event kernel stays synchronous and single-threaded
  (docs/archive/plan.md section 1). Only the async boundary may name tokio; if honba-engine
  ever grows a tokio dependency, the single-writer guarantee the whole
  determinism argument rests on is gone.
- WASM purity. honba-api-wasm compiles to wasm32-unknown-unknown, which has no
  filesystem and no tokio. Anything it needs must already be a pure L0-L4 crate
  (docs/archive/plan.md section 4.4).
"""

import sys
from pathlib import Path

import tomllib

# L0: honba-messages
# L1: honba-entities
# L2: honba-market, honba-ports, honba-risk
# L3: honba-engine, honba-indicators, honba-async
# L4: honba-sim, honba-strategy, honba-signals, honba-portfolio
# L5: honba-analytics, honba-feed, honba-sweep, honba-codegen, honba-config
# L6: honba-api, honba-testing
# L7: honba-py, honba-cli, honba-api-rest, honba-api-wasm
# Brokers are not crates: they live in the honba-adapters repo (Python).

ALLOWED_PROD = {
    "honba-messages": set(),
    "honba-entities": {"honba-messages"},
    "honba-market": {"honba-entities", "honba-messages"},
    "honba-ports": {"honba-messages", "honba-entities", "honba-market"},
    # Pure pre-trade risk stage (ADR 0018). Reads honba-market's rules (default features only:
    # never the `india` pack, rule 4 below) and must stay tokio-free.
    "honba-risk": {"honba-messages", "honba-entities", "honba-market"},
    "honba-engine": {"honba-messages", "honba-entities", "honba-risk"},  # Engine::submit gate
    "honba-indicators": {"honba-messages", "honba-entities"},
    # Explainable buy/sell detectors (ADR 0031). Pure and causal: composes the
    # honba-indicators engine over the honba-entities signal contract.
    "honba-signals": {"honba-messages", "honba-entities", "honba-indicators"},
    # Pure portfolio valuation (ADR 0032). Values positions and marks into the
    # honba-entities portfolio report contract: no clock, no I/O, no orders.
    "honba-portfolio": {"honba-messages", "honba-entities"},
    "honba-async": {"honba-messages", "honba-engine", "honba-ports"},
    "honba-sim": {
        "honba-messages",
        "honba-entities",
        "honba-engine",
        # Generic cost schedules only (never the `india` pack): the simulator charges a
        # `CostSchedule` per fill (E3-S3).
        "honba-market",
    },
    "honba-strategy": {
        "honba-engine",
        "honba-indicators",
        "honba-messages",
        "honba-entities",
        "honba-risk",  # StrategyRunner gate
    },
    "honba-analytics": {"honba-messages", "honba-entities"},
    "honba-feed": {
        "honba-messages",
        "honba-entities",
        "honba-engine",
        "honba-analytics",
        "honba-ports",
    },
    "honba-sweep": {
        "honba-messages",
        "honba-entities",
        "honba-engine",
        "honba-strategy",
        "honba-sim",
        "honba-feed",
        "honba-analytics",
    },
    "honba-config": {"honba-messages", "honba-strategy", "honba-risk"},  # RiskLimits re-export
    "honba-codegen": {
        "honba-messages",
        "honba-entities",
        "honba-strategy",
        "honba-config",
        "honba-api",
    },
    "honba-api": {"honba-messages", "honba-entities", "honba-strategy", "honba-indicators", "honba-analytics"},
    "honba-testing": {
        "honba-engine",
        "honba-messages",
        "honba-entities",
        "honba-ports",
        "honba-sim",
    },
    "honba-py": {
        "honba-messages",
        "honba-entities",
        "honba-market",
        "honba-engine",
        "honba-strategy",
        "honba-sim",
        "honba-codegen",
        # The in-process SDK transport drives the served router (no handler is reimplemented).
        "honba-api-rest",
        "honba-risk",   # bindings (ADR 0018 decision 10)
        "honba-async",  # operator Command binding (E2-S1 rest)
        # PythonGateway: a Python broker adapter as a honba_ports::ExecutionGateway (E3-S7, ADR 0030).
        "honba-ports",
    },
    "honba-cli": {
        "honba-messages",
        "honba-entities",
        "honba-market",
        "honba-engine",
        "honba-analytics",
        "honba-feed",
        "honba-strategy",
        "honba-testing",
        "honba-sim",
        "honba-codegen",
        "honba-api",
        "honba-api-rest",
        "honba-risk",   # CLI backtest assembler
        "honba-async",  # operator Command::State in `trading-state set`
        "honba-config",  # FillModel/ExecutionConfig for `serve --fill-model` (ADR 0030)
        "honba-ports",  # TickReader for `serve --ticks` (ADR 0030)
    },
    "honba-api-rest": {
        "honba-api",
        "honba-messages",
        "honba-entities",
        "honba-feed",
        "honba-market",
        "honba-ports",
        "honba-risk",  # write-route gate
        # Run executor edges (ADR 0017 decision 8). `honba-sweep` added for sweep execution (E4-S5);
        # `honba-async` is deliberately absent (workers are std threads).
        "honba-engine",  # L3: kernel, honba_engine::Clock
        "honba-strategy",  # L4: Strategy, StrategyIr
        "honba-sim",  # L4: fill/execution simulation
        "honba-analytics",  # L5: BacktestMetrics
        "honba-portfolio",  # L4: portfolio valuation for GET /portfolio (ADR 0032, E13-S2)
        "honba-config",  # L5: ExecutionConfig, AccountConfig, BacktestRunConfig validation
        "honba-sweep",  # L5: concurrent sweep execution
    },
    # Plan 4.4: pure L0-L4 only. The wasm surface computes indicators, screener
    # predicates, and replay; it has no filesystem and must stay replay-only.
    # E11-S5: it names the screener domain types (honba-entities, L1) and builds
    # honba-messages bars (L0) to feed the honba-indicators evaluator (L3).
    # E11-S5 (replay): it parses Journal v1 NDJSON with honba-engine's AuditLog /
    # JournalV1 / ReplayState (L3). All are pure; the NDJSON crosses as a string.
    "honba-api-wasm": {
        "honba-messages",
        "honba-entities",
        "honba-engine",
        "honba-indicators",
    },
}

# Dev-dependencies: no upward edges (a lower tier may not reach a higher one).
ALLOWED_DEV = {
    "honba-messages": set(),
    "honba-entities": set(),
    # Integration test drives a simulator fill with the India schedule (E3-S3).
    "honba-market": {"honba-engine", "honba-sim"},
    "honba-ports": {"honba-testing"},
    "honba-risk": set(),
    "honba-engine": {"honba-sim", "honba-market"},  # tests name InstrumentRules for a RulesSource
    "honba-indicators": set(),
    "honba-signals": set(),
    "honba-portfolio": set(),
    "honba-sim": set(),
    "honba-strategy": {
        "honba-analytics",
        "honba-feed",
        "honba-market",  # tests name InstrumentRules for a RulesSource
        "honba-sim",
        "honba-testing",
    },
    "honba-testing": set(),
    "honba-analytics": set(),
    "honba-feed": set(),
    "honba-codegen": set(),
    "honba-config": set(),
    "honba-api": set(),
    "honba-py": set(),
    "honba-cli": {"honba-ports"},  # TickReader in the `--ticks` loader tests
    "honba-async": {"honba-testing", "honba-sim"},
    "honba-sweep": {"honba-testing"},
    "honba-api-rest": {"honba-testing"},
    "honba-api-wasm": set(),
}

# Core crates that must NEVER enable market-specific packs (like the `india` feature of honba-market)
CORE_CRATES = {
    "honba-messages",
    "honba-entities",
    "honba-risk",
    "honba-engine",
    "honba-indicators",
    "honba-signals",
    "honba-portfolio",
    "honba-sim",
    "honba-strategy",
    "honba-analytics",
    "honba-feed",
    "honba-testing",
}

# Sync kernel crates that must NOT depend on any async/runtime crates (async isolation + sync kernel purity)
# These are the "pure" crates that must stay synchronous and tokio-free.
SYNC_KERNEL_CRATES = {
    "honba-messages",
    "honba-entities",
    "honba-risk",
    "honba-engine",
    "honba-indicators",
    "honba-signals",
    "honba-portfolio",
    "honba-sim",
    "honba-strategy",
    "honba-market",
    "honba-analytics",
}

# Crates allowed to depend on tokio in PRODUCTION. The event kernel is absent
# on purpose: docs/archive/plan.md 1 keeps the loop synchronous and single-threaded.
ASYNC_BOUNDARY_CRATES = {
    "honba-async",
    "honba-sweep",
    "honba-feed",
    "honba-testing",
    "honba-py",
    "honba-cli",
    "honba-api-rest",
}

# Crates that target wasm32-unknown-unknown and therefore must not pull tokio
# (which has no wasm32 build) or any filesystem/network crate.
WASM_CRATES = {"honba-api-wasm"}

# External crates that a wasm32 target cannot satisfy.
WASM_FORBIDDEN_DEPS = {
    "tokio",
    "reqwest",
    "hyper",
    "axum",
    "ureq",
    "sqlx",
    "std::fs",
    "fs",
    "tempfile",
    "pyo3",
    "openssl",
}


def check_crate(d) -> int:
    """Report every rule this crate breaks; return the violation count."""
    f = d / "Cargo.toml"
    if not f.exists():
        return 0
    data = tomllib.loads(f.read_text())
    name = data.get("package", {}).get("name", d.name)

    if name not in ALLOWED_PROD:
        print(f"VIOLATION: {name} is not registered in the layering tables")
        return 1

    errs = 0

    # 1. Production dependencies must point inward.
    prod_deps = {k for k in data.get("dependencies", {}) if k.startswith("honba-")}
    bad_prod = prod_deps - ALLOWED_PROD[name]
    if bad_prod:
        print(
            f"VIOLATION: {name} -> {sorted(bad_prod)} (production dependency not allowed)"
        )
        errs += 1

    # 2. Dev-dependencies must not reach upward (no honba-sim -> honba-testing).
    dev_deps = {k for k in data.get("dev-dependencies", {}) if k.startswith("honba-")}
    bad_dev = dev_deps - ALLOWED_DEV[name]
    if bad_dev:
        print(
            f"VIOLATION: {name} -> {sorted(bad_dev)} (dev-dependency not allowed / upward edge)"
        )
        errs += 1

    all_dep_names = set(data.get("dependencies", {})) | set(
        data.get("dev-dependencies", {})
    )

    # 3. pyo3 is contained in honba-py.
    if "pyo3" in all_dep_names and name != "honba-py":
        print(
            f"VIOLATION: {name} depends on pyo3 (pyo3 is restricted to honba-py only)"
        )
        errs += 1

    # 4. Core crates never enable a market-specific pack.
    for section_name in ("dependencies", "dev-dependencies"):
        section = data.get(section_name, {})
        if "honba-market" in section and name in CORE_CRATES:
            dep_info = section["honba-market"]
            if isinstance(dep_info, dict) and "india" in dep_info.get("features", []):
                print(
                    f"VIOLATION: core crate {name} enables 'india' feature of honba-market"
                )
                errs += 1

    # 5. Async isolation: only the boundary may depend on tokio in production.
    if (
        "tokio" in set(data.get("dependencies", {}))
        and name not in ASYNC_BOUNDARY_CRATES
    ):
        print(
            f"VIOLATION: {name} depends on tokio in production "
            f"(async-isolation rule: only {sorted(ASYNC_BOUNDARY_CRATES)} may)"
        )
        errs += 1

    # 6. Sync kernel purity: these crates must stay free of async/runtime crates.
    if name in SYNC_KERNEL_CRATES:
        for dep in all_dep_names:
            if dep.endswith(("-async", "-rest")):
                print(
                    f"VIOLATION: {name} depends on {dep} "
                    "(sync kernel crates may not depend on async/runtime crates)"
                )
                errs += 1

    # 7. WASM purity: a wasm32 target has no tokio, no filesystem, no network.
    if name in WASM_CRATES:
        for dep in all_dep_names & WASM_FORBIDDEN_DEPS:
            print(
                f"VIOLATION: {name} depends on {dep} "
                "(wasm32-unknown-unknown has no tokio, filesystem, or network)"
            )
            errs += 1

    return errs


def main() -> int:
    root = Path(__file__).resolve().parent.parent / "crates"
    errs = 0
    for d in sorted(root.iterdir()):
        errs += check_crate(d)
    if errs:
        return 1
    print("Dependency hierarchy and layering rules OK.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
