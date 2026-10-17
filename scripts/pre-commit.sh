#!/usr/bin/env bash
set -euo pipefail

# Determine REPO_ROOT
if git rev-parse --show-toplevel >/dev/null 2>&1; then
    REPO_ROOT="$(git rev-parse --show-toplevel)"
else
    # Invoked as hook from .git/hooks/
    HOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    REPO_ROOT="$(cd "$HOOK_DIR/../.." && pwd)"
fi

WORKSPACE_ROOT="$(cd "$REPO_ROOT/.." && pwd)"

VENV_BIN=""
if [ -d "$WORKSPACE_ROOT/.venv/bin" ]; then
    VENV_BIN="$WORKSPACE_ROOT/.venv/bin"
elif [ -d "$REPO_ROOT/.venv/bin" ]; then
    VENV_BIN="$REPO_ROOT/.venv/bin"
fi

echo "🔍 Running pre-commit verification in $REPO_ROOT..."

# 1. Rust checks if Cargo.toml exists
if [ -f "$REPO_ROOT/Cargo.toml" ]; then
    echo "🦀 Checking Rust formatting..."
    (cd "$REPO_ROOT" && cargo fmt --all --check)
    echo "🦀 Checking Rust clippy & compiler warnings..."
    (cd "$REPO_ROOT" && cargo clippy --workspace --all-targets -- -D warnings)
fi

# 2. Python checks
RUFF_CMD="ruff"
if [ -n "$VENV_BIN" ] && [ -x "$VENV_BIN/ruff" ]; then
    RUFF_CMD="$VENV_BIN/ruff"
fi

TARGET_DIR="."
if [ -d "$REPO_ROOT/python" ]; then
    TARGET_DIR="python"
fi

if command -v "$RUFF_CMD" >/dev/null 2>&1; then
    echo "🐍 Checking Ruff lints in $TARGET_DIR..."
    (cd "$REPO_ROOT" && "$RUFF_CMD" check "$TARGET_DIR")
    echo "🐍 Checking Ruff formatting in $TARGET_DIR..."
    (cd "$REPO_ROOT" && "$RUFF_CMD" format --check "$TARGET_DIR")
else
    echo "⚠️ ruff not found, skipping ruff check"
fi

# 3. Pyright check
PYRIGHT_TARGET="$REPO_ROOT"
if [ -d "$REPO_ROOT/python/src" ]; then
    PYRIGHT_TARGET="$REPO_ROOT/python/src"
fi

if command -v npx >/dev/null 2>&1; then
    echo "🔍 Checking Pyright type & interpreter diagnostics..."
    (cd "$WORKSPACE_ROOT" && npx pyright "$PYRIGHT_TARGET")
elif command -v pyright >/dev/null 2>&1; then
    echo "🔍 Checking Pyright type & interpreter diagnostics..."
    (cd "$WORKSPACE_ROOT" && pyright "$PYRIGHT_TARGET")
fi

echo "✅ Pre-commit verification passed: 0 lint/interpreter errors/warnings."
