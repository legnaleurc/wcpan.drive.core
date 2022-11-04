PYTHON := poetry run python3

PKG_FILES := pyproject.toml poetry.lock
PKG_DIR := .venv
BLD_LOCK := $(PKG_DIR)/pyvenv.cfg

.PHONY: all venv clean upload test install

all: venv

clean:
	rm -rf ./dist ./build ./*.egg-info

purge: clean
	rm -rf $(PKG_DIR)

test: venv
	$(PYTHON) -m compileall wcpan
	$(PYTHON) -m unittest

build: clean venv
	poetry build

publish: venv
	poetry publish

venv: $(BLD_LOCK)

$(BLD_LOCK): $(PKG_FILES)
	poetry install
	touch $@
