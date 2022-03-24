.PHONY: all ensure-poetry install version

POETRY_INSTALLED := $(shell command -v poetry)


all: version


ensure-poetry:
ifndef POETRY_INSTALLED
ifndef VIRTUAL_ENV
	$(error You should be in a virtual environment before running Make commands.)
endif
		@echo "Installing the Poetry package manager and dev requirements."
		python -m pip install --upgrade pip
		python -m pip install --upgrade poetry
endif

install: ensure-poetry
		@echo "Checking the package and dev requirements are installed."
		poetry install


version: install
	poetry run wingline --version


test: install
	poetry run nox


check-version: install
	poetry run invoke check-version


tag-release: install
	poetry run invoke tag-release

bump-version: install
	poetry version patch | tail -n 1 | sed 's/^Bumping/Bump/'
