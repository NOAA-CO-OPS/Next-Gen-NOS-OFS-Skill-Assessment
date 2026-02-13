ENV_NAME = ofs_dps
CONDA_RUN = conda run --no-banner -n $(ENV_NAME)

# Use mamba if available, fall back to conda.
# Check multiple locations since Make's shell doesn't source .bashrc,
# so conda/mamba may not be on the default PATH.
SOLVER := $(shell \
	if command -v mamba >/dev/null 2>&1; then echo mamba; \
	elif [ -x "$$(conda info --base 2>/dev/null)/bin/mamba" ]; then echo "$$(conda info --base)/bin/mamba"; \
	elif [ -x "$$CONDA_PREFIX/bin/mamba" ]; then echo "$$CONDA_PREFIX/bin/mamba"; \
	elif [ -x "$$MAMBA_ROOT_PREFIX/bin/mamba" ]; then echo "$$MAMBA_ROOT_PREFIX/bin/mamba"; \
	elif [ -x "$$CONDA_EXE" ] && "$$(dirname $$CONDA_EXE)/mamba" --version >/dev/null 2>&1; then echo "$$(dirname $$CONDA_EXE)/mamba"; \
	else echo conda; \
	fi)

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
	@echo "Using solver: $(SOLVER)"
	@if conda env list | grep -q "$(ENV_NAME)"; then \
		echo "Environment '$(ENV_NAME)' exists. Updating..."; \
		$(SOLVER) env update -f environment.yml -n $(ENV_NAME) --prune; \
	else \
		echo "Environment '$(ENV_NAME)' not found. Creating..."; \
		$(SOLVER) env create -f environment.yml -n $(ENV_NAME) --yes; \
	fi

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
	@echo "CONDA_EXE:  $$CONDA_EXE"
	@echo "CONDA_PREFIX: $$CONDA_PREFIX"
	@echo "MAMBA_ROOT_PREFIX: $$MAMBA_ROOT_PREFIX"

## Remove the conda environment
clean:
	conda env remove -n $(ENV_NAME) --yes
