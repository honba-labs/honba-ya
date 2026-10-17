#!/usr/bin/env sh
# Create the git-ignored .env from the committed template, readable by you only.
set -eu
cd "$(dirname "$0")/.."
if [ -e .env ]; then
  echo ".env already exists; leaving it untouched" >&2
else
  umask 077
  cp .env.example .env
  echo "created .env (mode 600); fill in your broker credentials" >&2
fi
chmod 600 .env
