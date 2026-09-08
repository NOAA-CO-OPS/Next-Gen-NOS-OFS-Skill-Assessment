# Fast local gate for Windows PowerShell (same checks as scripts/ci-local.sh).
# Not the full CI matrix. Requires the ofs_dps / editable .[dev] environment.
#
# Usage (from repo root):
#   powershell -File scripts/ci-local.ps1
#   # or, in PowerShell:
#   .\scripts\ci-local.ps1
#
# Alternative: Git Bash + `bash scripts/ci-local.sh` or `make ci-local`.

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

$RuffPin = "0.7.0"

Write-Host "==> preflight (import ofs_skill)"
python -c "import ofs_skill" 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Error @"
ci-local: cannot import ofs_skill.
Activate the dev environment first (e.g. 'conda activate ofs_dps'),
then from this checkout: pip install -e '.[dev]'
"@
}

Write-Host "==> ruff (pinned $RuffPin, same as CI / pre-commit)"
$ruffOut = python -m ruff --version 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Error "ci-local: ruff not found. Install: pip install 'ruff==$RuffPin' (or pip install -e '.[dev]')."
}
$ruffVer = ($ruffOut -split '\s+')[1]
if ($ruffVer -ne $RuffPin) {
    Write-Error @"
ci-local: ruff $ruffVer != pinned $RuffPin (CI uses $RuffPin).
Newer ruff can report extra rules. Install: pip install 'ruff==$RuffPin'
"@
}
python -m ruff check src bin
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "==> detect-secrets"
$files = git ls-files
if (-not $files) {
    Write-Error "ci-local: git ls-files returned no files."
}
# detect-secrets-hook accepts path arguments (same baseline as bash gate).
& detect-secrets-hook --baseline .secrets.baseline @files
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "==> fast pytest subset (package imports)"
pytest tests/test_package_imports.py -q `
  -m "not network and not manual" `
  -o addopts=""
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "ci-local passed."
