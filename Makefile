MAKEFLAGS += -j10

.PHONY: precommit build test dev-legacy

YEAR ?= $(shell date +%Y)

precommit:
	uv run ruff format
	uv run ruff check

build: precommit
	PYTHONPATH=py uv run python -m gh_trending_analytics build --kind repository --year $(YEAR)
	PYTHONPATH=py uv run python -m gh_trending_analytics build --kind developer --year $(YEAR)

test: precommit
	uv run python -m pytest -q

dev-legacy: precommit
	PYTHONPATH=py:legacy uv run python -m gh_trending_web --analytics ./analytics --port 8000
