# Test Suite

Automated tests for the Next-Gen NOS OFS Skill Assessment package live in this directory.

## Layout

```
tests/
├── README.md                 # This file
├── fixtures/                 # Snapshot / sample data for offline tests
│   └── pipeline/             # Mentor production-trimmed .ctl/.obs/.prd/.int
├── helpers/                  # Shared mocks (USGS / CO-OPS / NDBC)
├── manual/                   # Opt-in live audits (@pytest.mark.manual)
├── requirements-test.txt     # Optional extra test tooling
└── *_test.py / test_*.py     # Test modules (including integration_*)
```

## Run tests locally

From the repository root, with the conda env activated (`ofs_dps`).

**Before pytest on a PR branch**, reinstall the package editable from *this*
checkout so you are not accidentally using another clone’s install:

```bash
pip install -e ".[dev]"
```

If you skip that and the env still points at another checkout, the first
failure can look severe (`ImportError` for symbols such as `redact_secrets`,
or coverage stuck at **0%**) even though the branch itself is fine.

```bash
# Full default suite (uses pyproject.toml addopts, including coverage HTML)
pytest

# Match CI: unit + integration; exclude live network / manual audits
pytest -m "not network and not manual" -n auto \
  -o addopts="" \
  --cov=src/ofs_skill --cov-report=term-missing

# Integration-only (fixture / mock pipeline boundaries)
pytest -m integration -o addopts=""

# Fast smoke used by `make ci-local` / pre-push
pytest tests/test_package_imports.py -q -o addopts="" -m "not network and not manual"

# Live USGS (and other) network tests — local only if you have API_USGS_PAT
API_USGS_PAT=... pytest -m network -o addopts=""
```

## Markers

Declared in `pyproject.toml`:

| Marker | Purpose |
|--------|---------|
| `network` | Needs live network / APIs — **not** run in normal CI |
| `manual` | Opt-in live audits — run with `-m manual` |
| `integration` | Fixture-based pipeline tests (no live network) — run on every PR |

## Continuous Integration

**Every PR** (`.github/workflows/ci.yml`) runs in parallel:

1. **lint** — `ruff` + `detect-secrets`
2. **types** — `mypy` on `src/ofs_skill`
3. **docs** — `mkdocs build --strict`
4. **package** — `python -m build` + `twine check`
5. **tests** — micromamba + `pytest -m "not network and not manual"`
   (unit **and** integration; mocked USGS included; no internet/API keys required).
   Coverage: `coverage.xml` artifact on Ubuntu/3.11; CI fails below
   `fail_under` in `pyproject.toml` (repo-only gate; no Coveralls).

**Scheduled / manual only** (`.github/workflows/network-tests.yml`):

- `pytest -m network` with encrypted secret `API_USGS_PAT`
- Trigger: weekly cron (Sunday 17:00 UTC ≈ 12:00 PM US Eastern) or `workflow_dispatch`
- Step summary records duration / exit for Actions monitoring; nightly cron deferred until weekly is stable

**Docs Pages** (`.github/workflows/docs-pages.yml`): deploy MkDocs from `main` (org must enable Pages → GitHub Actions).

After a local CI-equivalent run, print the coverage summary:

```bash
python scripts/coverage_summary.py coverage.xml --fail-under-from-pyproject
```

See [fixtures/README.md](fixtures/README.md) for fixture layout and refresh steps.
See [CONTRIBUTING.md](../CONTRIBUTING.md) for setup and `make ci-local`.

## Adding tests

1. Prefer offline unit/integration tests with mocks/fixtures over live API calls
2. Mark live checks with `@pytest.mark.network` or `@pytest.mark.manual`
3. Mark fixture-based pipeline boundary tests with `@pytest.mark.integration`
4. Reuse `tests/helpers/api_mocks.py` for USGS / CO-OPS / NDBC HTTP stubs
5. Name files `*_test.py` or `test_*.py` (pre-commit `name-tests-test`)
6. Run the relevant file locally before opening a PR
