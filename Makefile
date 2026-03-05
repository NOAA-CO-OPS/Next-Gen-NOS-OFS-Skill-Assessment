ENV_NAME = ofs_dps
CONDA_RUN = $(_CONDA_EXE_FWD) run -n $(ENV_NAME)

# ---------- cross-platform solver detection (prefer mamba) ----------
# Derive paths from CONDA_EXE — set by all conda distros (anaconda,
# miniconda, miniforge, mambaforge) during `conda init`, regardless
# of install location.
_CONDA_EXE_FWD := $(subst \,/,$(CONDA_EXE))
_CONDA_DIR     := $(if $(_CONDA_EXE_FWD),$(dir $(_CONDA_EXE_FWD)),)
_CONDA_BASE    := $(if $(_CONDA_DIR),$(dir $(patsubst %/,%,$(_CONDA_DIR))),)

ifeq ($(OS),Windows_NT)
    # Windows: pure $(wildcard) — no shell dependency (works with cmd.exe).
    _MAMBA_FOUND := $(or \
        $(wildcard $(_CONDA_DIR)mamba.exe),\
        $(wildcard $(_CONDA_BASE)Library/bin/mamba.exe))
    ifneq ($(_MAMBA_FOUND),)
        SOLVER := $(firstword $(_MAMBA_FOUND))
    else
        SOLVER := $(_CONDA_EXE_FWD)
    endif
    # Detect env before setting SHELL so $(shell) uses cmd.exe (always available)
    _ENV_EXISTS := $(findstring $(ENV_NAME),$(shell "$(SOLVER)" env list 2>NUL))
    # Recipes use POSIX syntax; Git-for-Windows provides sh.exe.
    SHELL := sh
else
    # Unix: check PATH, then relative to conda/mamba env vars.
    SOLVER := $(or \
        $(shell command -v mamba 2>/dev/null),\
        $(wildcard $(_CONDA_BASE)bin/mamba),\
        $(wildcard $(CONDA_PREFIX)/bin/mamba),\
        $(wildcard $(MAMBA_ROOT_PREFIX)/bin/mamba),\
        $(_CONDA_EXE_FWD))
    _ENV_EXISTS := $(findstring $(ENV_NAME),$(shell $(SOLVER) env list 2>/dev/null))
endif

.DEFAULT_GOAL := help

.PHONY: help env install pre-commit setup info clean

## Show available targets
help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@echo "  setup        Full developer setup (env + install + pre-commit)"
	@echo "  env          Create or update the conda environment"
	@echo "  install      Install the package in development mode (pip install -e .[dev])"
	@echo "  pre-commit   Install pre-commit git hooks"
	@echo "  info         Show detected solver and environment info"
	@echo "  clean        Remove the conda environment"
	@echo ""
	@echo "Solver: $(SOLVER)"

## Create or update the conda environment from environment.yml
env:
	@echo Using solver: $(SOLVER)
ifneq ($(_ENV_EXISTS),)
	@echo Environment '$(ENV_NAME)' exists. Updating...
	$(SOLVER) env update -f environment.yml -n $(ENV_NAME) --prune
else
	@echo Environment '$(ENV_NAME)' not found. Creating...
	$(SOLVER) env create -f environment.yml -n $(ENV_NAME) --yes
endif

## Install the package in development mode
install:
	$(CONDA_RUN) pip install -e ".[dev]"

## Install pre-commit hooks into the local .git/hooks
pre-commit:
	$(CONDA_RUN) pre-commit install

## Full developer setup: create/update env, install package, install hooks
setup: env install pre-commit
	@echo "Setup complete. Activate with: conda activate $(ENV_NAME)"

## Show which solver (mamba/conda) was detected
info:
	@echo "Solver:     $(SOLVER)"
	@echo "Environment: $(ENV_NAME)"
	@echo "CONDA_EXE:  $(CONDA_EXE)"
	@echo "CONDA_PREFIX: $(CONDA_PREFIX)"
	@echo "MAMBA_ROOT_PREFIX: $(MAMBA_ROOT_PREFIX)"

## Remove the conda environment
clean:
	$(SOLVER) env remove -n $(ENV_NAME) --yes
