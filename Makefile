.PHONY: setup test check run

# Override BOOTSTRAP_PYTHON if python3 is not on PATH. No activation is needed.
BOOTSTRAP_PYTHON ?= python3

setup test check run:
	$(BOOTSTRAP_PYTHON) scripts/dev.py $@
