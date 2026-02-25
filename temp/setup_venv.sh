#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
python3 -m venv .venv
./.venv/bin/pip install --upgrade pip
./.venv/bin/pip install -r day07/requirements.txt

echo "Virtualenv created at temp/.venv and requirements installed."
