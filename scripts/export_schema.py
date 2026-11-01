#!/usr/bin/env python3
"""Repo-checkout wrapper: regenerate schema/domain and, optionally, the derived artifacts.

Source of truth is Rust (`honba-tamashii-codegen`), invoked through the unified
Python `honba` CLI (`honba schema export`).

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
workspace_root = honba_root.parent


def honba_cli() -> list[str]:
    venv_honba = workspace_root / ".venv" / "bin" / "honba"
    if venv_honba.is_file():
        return [str(venv_honba)]
    found = shutil.which("honba")
    if found is not None:
        return [found]
    return [sys.executable, "-m", "honba.cli.main"]


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

    cmd = [
        *honba_cli(),
        "schema",
        "export",
        "--out-dir",
        str(honba_root / "schema" / "domain"),
    ]
    if args.frontend_dir is not None:
        cmd += ["--ts", "--ts-out-dir", str(Path(args.frontend_dir))]
    else:
        cmd += ["--no-ts"]
    if args.openapi_dir is not None:
        cmd += ["--openapi-dir", str(Path(args.openapi_dir))]
    if args.pyi_dir is not None:
        cmd += ["--pyi-dir", str(Path(args.pyi_dir))]
    print("$", " ".join(cmd), file=sys.stderr)
    subprocess.check_call(cmd, cwd=honba_root)
    print("Codegen complete (Rust source of truth via `honba schema export`).", file=sys.stderr)


if __name__ == "__main__":
    main()
