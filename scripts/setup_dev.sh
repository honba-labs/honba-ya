#!/usr/bin/env bash
set -euo pipefail
cargo build --workspace
(cd python && pip install -e '.[dev]')
echo 'Honba dev environment ready.'
