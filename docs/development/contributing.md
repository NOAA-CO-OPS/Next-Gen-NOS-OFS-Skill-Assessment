# Contributing

## Developer setup

Recommended path (conda + Makefile):

```bash
# Requires conda/mamba with CONDA_EXE set (after conda init)
make setup
conda activate ofs_dps
```

What `make setup` does:
1. Create/update the `ofs_dps` conda env from `environment.yml`
2. `pip install -e ".[dev]"`
3. Install **pre-commit** and **pre-push** git hooks

Manual equivalent:

```bash
conda env create -n ofs_dps -f environment.yml   # or: conda env update -n ofs_dps -f environment.yml --prune
conda activate ofs_dps
pip install -e ".[dev]"
pre-commit install
pre-commit install --hook-type pre-push
```

Python support: **3.11+** (`requires-python` in `pyproject.toml`).

## Local checks before push

Pre-push runs a **fast** subset only (not the full CI matrix):

```bash
make ci-local
# or
bash scripts/ci-local.sh
```

**Windows:** use Git Bash for the commands above, or run the PowerShell
equivalent from the repo root:

```powershell
.\scripts\ci-local.ps1
```

This runs:
- `ruff check src bin` via **pinned** `ruff==0.7.0` (same as CI / pre-commit;
  newer ruff is rejected so you do not see spurious UP031/UP042 failures)
- `detect-secrets` against `.secrets.baseline`
- a small pytest subset (`tests/test_package_imports.py`)

If `ci-local` fails, the push is blocked. Fix locally and try again.
You can bypass with `git push --no-verify` (not recommended).

If imports fail or coverage looks like 0% because another checkout’s editable
install is active, run from **this** branch checkout:

```bash
pip install -e ".[dev]"
```

A stale editable install against a different clone is a common footgun: the
first symptom is often `ImportError: cannot import name 'redact_secrets'`
(or similar) and pytest reporting **0% coverage**, which looks like a hard
failure but is just the wrong `ofs_skill` on `sys.path`. Reinstalling
editable from this checkout fixes it.

## Tests and markers

Reinstall from this checkout first if you switched branches or clones
(`pip install -e ".[dev]"`) — see the note under [Local checks before push](#local-checks-before-push).

```bash
# Default local suite (includes coverage HTML via pyproject addopts)
pytest

# Match CI (unit + integration; no live network / manual audits; parallel)
pytest -m "not network and not manual" -n auto -o addopts="" --cov=src/ofs_skill --cov-report=term-missing

# Live USGS network tests (needs API_USGS_PAT; also run via network-tests.yml)
API_USGS_PAT=... pytest -m network -o addopts=""
```

| Marker | Meaning | When it runs |
|--------|---------|--------------|
| *(none)* | Normal unit tests | Every PR / local |
| `integration` | Fixture/mock pipeline tests (no live network) | Every PR |
| `network` | Live API / network access | Scheduled / `workflow_dispatch` only (secret `API_USGS_PAT`) |
| `manual` | Opt-in live audits | Opt-in only |

Offline mocks and pipeline sample data: `tests/helpers/api_mocks.py`, `tests/fixtures/` (see `tests/fixtures/README.md`).

## CI expectations

GitHub Actions runs **in parallel** on PRs (`.github/workflows/ci.yml`):

| Job | What |
|-----|------|
| **lint** | `ruff` + `detect-secrets` (pip-only; no full conda env) |
| **types** | `mypy` on `src/ofs_skill` (pip-only) |
| **docs** | `mkdocs build --strict` (pip-only) |
| **package** | `python -m build` + `twine check` (pip-only) |
| **tests** | micromamba from `environment.yml` + pytest (`not network and not manual`) + coverage gate |

Live network tests: `.github/workflows/network-tests.yml` (weekly Sunday 17:00 UTC ≈ 12:00 PM US Eastern, or `workflow_dispatch`), using encrypted secret `API_USGS_PAT` as an env var. Do **not** write keys into `conf/api_keys.conf` or commit them. Scheduled runs skip if the secret is missing; manual runs fail loudly. Logs redact the token.

Docs site: `docs-pages.yml` deploys MkDocs to GitHub Pages from `main` (enable **Settings → Pages → Source = GitHub Actions**).

### Coverage gate (repo-only; no Coveralls)

CO-OPS/NOAA org policy may block third-party coverage hosts (e.g. Coveralls), so we keep coverage inside GitHub Actions:

- `pytest-cov` writes `coverage.xml` every run (never restored from cache)
- Ubuntu / Python 3.11 uploads it as the `coverage-xml` artifact
- CI fails if coverage is below `[tool.coverage.report] fail_under` in `pyproject.toml` (initial floor **39**, matching the ~40% baseline)
- PRs that **lower** `fail_under` vs the base branch fail — ratchet up only, with maintainer approval to lower
- Optional local summary: `python scripts/coverage_summary.py coverage.xml --fail-under-from-pyproject`

Caching:
- Micromamba env/downloads keyed by OS + `environment.yml` hash (`-v2` bust suffix)
- Pip cache for lint/types/docs
- Pre-commit cache keyed by `.pre-commit-config.yaml`
- **Do not** cache `coverage.xml`

## Docs

Developer docs (MkDocs + mkdocstrings) live under `docs/` and are generated from
docstrings in `src/ofs_skill/`. User / theoretical guides stay in the
[project wiki](https://github.com/NOAA-CO-OPS/dev-Next-Gen-NOS-OFS-Skill-Assessment/wiki).

```bash
pip install -e ".[docs]"
mkdocs serve          # local preview
mkdocs build --strict # same gate as CI
```

## Packaging check (same as CI)

```bash
pip install -e ".[packaging]"
python -m build
twine check dist/*
```

Git deps `searvey` and `coastalmodeling-vdatum` are pinned to immutable commit
SHAs in both `pyproject.toml` and `environment.yml` (release tags kept in
comments) — keep those pins synchronized when bumping.

## Pull requests

1. Branch from an up-to-date `main`
2. Run `make ci-local` (or rely on pre-push)
3. Open a PR — wait for lint, types, docs, and tests
4. Keep PRs focused; do not commit secrets (`conf/api_keys.conf` is gitignored)
