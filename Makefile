.PHONY: up down db migrate seed install dev test lint playwright

# ── Docker ───────────────────────────────────────────────────────────────────
up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

# ── Local dev setup ──────────────────────────────────────────────────────────
install:
	pip install -e ".[dev]"
	playwright install chromium

dev:
	uvicorn app.api.main:app --reload --port 8000

worker:
	python workers/worker.py

# ── Database ──────────────────────────────────────────────────────────────────
migrate:
	alembic upgrade head

rollback:
	alembic downgrade -1

seed:
	python -m app.jobs.seed

# ── Quality ───────────────────────────────────────────────────────────────────
test:
	pytest -v

lint:
	ruff check app tests
	ruff format --check app tests

fmt:
	ruff format app tests
	ruff check --fix app tests
