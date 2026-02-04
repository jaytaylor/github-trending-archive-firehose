MAKEFLAGS += -j10

YEAR ?= $(shell date +%Y)

.PHONY: precommit
precommit:
	uv run ruff format
	uv run ruff check

.PHONY: build
build: precommit
	PYTHONPATH=py uv run python -m gh_trending_analytics build --kind repository --year $(YEAR)
	PYTHONPATH=py uv run python -m gh_trending_analytics build --kind developer --year $(YEAR)

.PHONY: test
test: precommit
	uv run python -m pytest -q

.PHONY: dev-legacy
dev-legacy: precommit
	PYTHONPATH=py:legacy uv run python -m gh_trending_web --analytics ./analytics --port 8000
