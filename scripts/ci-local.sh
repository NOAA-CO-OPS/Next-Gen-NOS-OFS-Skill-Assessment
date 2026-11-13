#!/usr/bin/env bash
# Fast local gate for pre-push / `make ci-local`.
# Not the full CI matrix (no conda rebuild, no Windows/macOS, no full suite).
#
# On Windows, use Git Bash for this script, or run scripts/ci-local.ps1 in
# PowerShell. Requires the same pinned tooling as CI (see pyproject [dev]).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

RUFF_PIN="0.7.0"

# Preflight: this gate needs the project dev environment (ruff, pytest,
# and an importable ofs_skill). Fail early with guidance instead of a
# wall of ModuleNotFoundError from pytest.
if ! python -c "import ofs_skill" >/dev/null 2>&1; then
  echo "ci-local: cannot import ofs_skill with python=$(command -v python || echo '<not found>')." >&2
  echo "Activate the dev environment first (e.g. 'conda activate ofs_dps'," >&2
  echo "or run 'make setup' / 'pip install -e .[dev]' from this checkout)." >&2
  exit 1
fi

echo "==> ruff (pinned ${RUFF_PIN}, same as CI / pre-commit)"
if ! python -m ruff --version >/dev/null 2>&1; then
  echo "ci-local: ruff not found for $(command -v python)." >&2
  echo "Install the pinned tool: pip install 'ruff==${RUFF_PIN}'  (or: pip install -e '.[dev]')." >&2
  exit 1
fi
RUFF_VER="$(python -m ruff --version | awk '{print $2}')"
if [[ "${RUFF_VER}" != "${RUFF_PIN}" ]]; then
  echo "ci-local: ruff ${RUFF_VER} != pinned ${RUFF_PIN} (CI uses ${RUFF_PIN})." >&2
  echo "Newer ruff can report extra rules (e.g. UP031/UP042). Install the pin:" >&2
  echo "  pip install 'ruff==${RUFF_PIN}'" >&2
  echo "or: pip install -e '.[dev]'" >&2
  exit 1
fi
python -m ruff check src bin

echo "==> detect-secrets"
git ls-files -z | xargs -0 detect-secrets-hook --baseline .secrets.baseline

echo "==> fast pytest subset (package imports)"
pytest tests/test_package_imports.py -q \
  -m "not network and not manual" \
  -o addopts=""

echo "ci-local passed."
