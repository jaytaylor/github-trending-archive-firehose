include Makefile

.PHONY: dev-legacy

dev-legacy: precommit
	PYTHONPATH=py:legacy uv run python -m gh_trending_web --analytics ./analytics --port 8000
