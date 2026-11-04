"""Crate layering and architectural guardrails for `honba-ya`.

Enforces:
1. Pure library workspace (Option B): zero Rust binary targets (`[[bin]]` or
   `src/main.rs`) in `honba-ya` — Python `honba` (`honba-tamashii`) is the
   single CLI entrypoint.
2. No upward dependency inversion: no `honba-ya` crate may depend on `pyo3`,
   `clap`, `honba-tamashii*`, or `honba-codegen`.
3. Inward crate layering across `honba-ya-*` and sibling workspace crates.
4. Sync purity: pure contract/strategy/config crates (`honba-ya-api`,
   `honba-ya-config`, `honba-ya-strategy`, `honba-ya-enhancer`) must not depend
   on `tokio` in production.
"""

import sys
from pathlib import Path

import tomllib

ALLOWED_PROD = {
    "honba-ya-api": {"honba-core"},
    "honba-ya-strategy": {
        "honba-core",
        "honba-indicators",
        "honba-mon",
    },
    "honba-ya-config": {
        "honba-core",
        "honba-mon",
        "honba-strategy",
        "honba-ya-strategy",
    },
    "honba-ya-persistence": {
        "honba-core",
        "honba-kokoro",
    },
    "honba-ya-testing": {
        "honba-core",
        "honba-te-core",
    },
    "honba-ya-actors": {
        "honba-core",
        "honba-kokoro",
        "honba-kokoro-ledger",
        "honba-kokoro-oms",
        "honba-kokoro-portfolio",
        "honba-mon",
        "honba-te-core",
        "honba-ya-persistence",
    },
    "honba-ya-backtest": {
        "honba-core",
        "honba-kokoro-ledger",
        "honba-kokoro-portfolio",
        "honba-mon",
        "honba-nou-agent",
        "honba-ya-strategy",
    },
    "honba-ya-enhancer": {
        "honba-core",
        "honba-mon",
        "honba-ya-actors",
        "honba-ya-backtest",
        "honba-ya-strategy",
    },
    "honba-ya-api-rest": {
        "honba-analytics",
        "honba-api",
        "honba-config",
        "honba-core",
        "honba-feed",
        "honba-market",
        "honba-mon",
        "honba-portfolio",
        "honba-strategy",
        "honba-testing",
    },
}

ALLOWED_DEV = {
    "honba-ya-api": set(),
    "honba-ya-strategy": {
        "honba-me-feed",
        "honba-me-market",
        "honba-ya-testing",
    },
    "honba-ya-config": set(),
    "honba-ya-persistence": set(),
    "honba-ya-testing": set(),
    "honba-ya-actors": {
        "honba-te-core",
        "honba-ya-testing",
    },
    "honba-ya-backtest": set(),
    "honba-ya-enhancer": set(),
    "honba-ya-api-rest": {
        "honba-testing",
        "honba-ya-testing",
    },
}

SYNC_PURE_CRATES = {
    "honba-ya-api",
    "honba-ya-config",
    "honba-ya-strategy",
    "honba-ya-enhancer",
}

ASYNC_BOUNDARY_CRATES = {
    "honba-ya-persistence",
    "honba-ya-testing",
    "honba-ya-actors",
    "honba-ya-backtest",
    "honba-ya-api-rest",
}

FORBIDDEN_DEPS = {
    "pyo3",
    "clap",
    "honba-tamashii-codegen",
    "honba-tamashii-py",
    "honba-codegen",
    "honba-py",
    "honba-ya-cli",
    "honba-cli",
}


def check_crate(d: Path) -> int:
    """Report every rule this crate breaks; return the violation count."""
    f = d / "Cargo.toml"
    if not f.exists():
        return 0
    data = tomllib.loads(f.read_text())
    name = data.get("package", {}).get("name", d.name)

    errs = 0

    # 1. Zero Rust binaries in honba-ya (Option B: single Python `honba` CLI).
    if data.get("bin") or (d / "src" / "main.rs").exists():
        print(
            f"VIOLATION: {name} defines a Rust binary target "
            "(`honba-ya` is pure library-only; CLI lives in `honba-tamashii`)"
        )
        errs += 1

    if name not in ALLOWED_PROD:
        print(f"VIOLATION: {name} is not registered in the layering tables")
        return errs + 1

    # 2. Production dependencies must point inward.
    prod_deps = {k for k in data.get("dependencies", {}) if k.startswith("honba-")}
    bad_prod = prod_deps - ALLOWED_PROD[name]
    if bad_prod:
        print(
            f"VIOLATION: {name} -> {sorted(bad_prod)} (production dependency not allowed)"
        )
        errs += 1

    # 3. Dev-dependencies must not reach upward.
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

    # 4. Forbidden dependencies (no PyO3, clap, or upward edges into honba-tamashii).
    bad_forbidden = all_dep_names & FORBIDDEN_DEPS
    if bad_forbidden:
        print(
            f"VIOLATION: {name} depends on forbidden crate(s) {sorted(bad_forbidden)}"
        )
        errs += 1

    # 5. Async isolation: only async boundary crates may depend on tokio in production.
    if (
        "tokio" in set(data.get("dependencies", {}))
        and name not in ASYNC_BOUNDARY_CRATES
    ):
        print(
            f"VIOLATION: {name} depends on tokio in production "
            f"(async-isolation rule: only {sorted(ASYNC_BOUNDARY_CRATES)} may)"
        )
        errs += 1

    if name in SYNC_PURE_CRATES:
        for dep in all_dep_names:
            if dep.endswith(("-rest", "-actors")) and (
                name != "honba-ya-enhancer" or dep != "honba-ya-actors"
            ):
                    print(
                        f"VIOLATION: {name} depends on {dep} "
                        "(sync pure crates may not depend on async server crates)"
                    )
                    errs += 1

    return errs


def main() -> int:
    root = Path(__file__).resolve().parent.parent / "crates"
    errs = 0
    for d in sorted(root.iterdir()):
        if d.is_dir():
            errs += check_crate(d)
    if errs:
        return 1
    print("Dependency hierarchy and layering rules OK.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
