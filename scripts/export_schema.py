#!/usr/bin/env python3
"""Repo-checkout wrapper: regenerate schema/domain and, optionally, the derived artifacts.

Source of truth is Rust (honba-codegen, via `honba schema export`); the Python
path is intentionally not used. This script finds `cargo` and the `honba` binary
and delegates to it, keeping the same CLI surface for backward compatibility.

    python3 scripts/export_schema.py                          # JSON bundle only
    python3 scripts/export_schema.py --frontend-dir DIR       # also emit DIR/domain.ts
    HONBA_FRONTEND_DIR=DIR python3 scripts/export_schema.py   # same, via environment
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

honba_root = Path(__file__).resolve().parents[1]


def honba_binary() -> Path:
    name = "honba"
    if sys.platform == "win32":
        name += ".exe"
    for d in (honba_root / "target/debug", honba_root / "target/release"):
        c = d / name
        if c.is_file():
            return c
    found = shutil.which(name)
    if found is not None:
        return Path(found)
    print("honba binary not found. Building with cargo...", file=sys.stderr)
    subprocess.check_call(["cargo", "build", "--bin", "honba"], cwd=honba_root)
    return honba_root / "target/debug" / name


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--frontend-dir",
        default=os.environ.get("HONBA_FRONTEND_DIR") or None,
        help="directory for generated domain.ts (default: $HONBA_FRONTEND_DIR; unset = skip)",
    )
    parser.add_argument(
        "--openapi-dir",
        default=os.environ.get("HONBA_OPENAPI_DIR") or None,
        help="directory for openapi.json (default: $HONBA_OPENAPI_DIR; unset = skip)",
    )
    parser.add_argument(
        "--pyi-dir",
        default=os.environ.get("HONBA_PYI_DIR") or None,
        help="directory for Python .pyi stubs (default: $HONBA_PYI_DIR; unset = skip)",
    )
    args = parser.parse_args(argv)

    binp = honba_binary()
    cmd = [
        str(binp),
        "schema",
        "export",
        "--output",
        str(honba_root / "schema" / "domain"),
    ]
    if args.frontend_dir is not None:
        cmd += ["--typescript", str(Path(args.frontend_dir))]
    if args.openapi_dir is not None:
        cmd += ["--openapi", str(Path(args.openapi_dir))]
    if args.pyi_dir is not None:
        cmd += ["--pyi", str(Path(args.pyi_dir))]
    print("$", " ".join(cmd), file=sys.stderr)
    subprocess.check_call(cmd, cwd=honba_root)
    print("Codegen complete (Rust source of truth).", file=sys.stderr)


if __name__ == "__main__":
    main()
