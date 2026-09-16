.DEFAULT_GOAL := help
SHELL := /bin/bash

PSQL := docker compose exec -T postgres psql -U orders -d orders

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: install
install: ## Local venv with dev dependencies
	python3 -m venv .venv
	./.venv/bin/pip install --upgrade pip
	./.venv/bin/pip install -r requirements-dev.txt
	./.venv/bin/pip install -e .

.PHONY: lint
lint: ## Ruff + mypy
	./.venv/bin/ruff check src tests
	./.venv/bin/ruff format --check src tests
	./.venv/bin/mypy

.PHONY: fmt
fmt: ## Autoformat
	./.venv/bin/ruff format src tests
	./.venv/bin/ruff check --fix src tests

.PHONY: test
test: ## Unit tests (no Kafka, no Postgres)
	./.venv/bin/pytest --cov=orders_stream --cov-report=term-missing

.PHONY: up
up: ## Start the stack
	docker compose up -d --build
	@echo "Console: http://localhost:8090   Metrics: http://localhost:9108/metrics"

.PHONY: down
down: ## Stop the stack, keep volumes
	docker compose down

.PHONY: clean
clean: ## Stop the stack and wipe volumes
	docker compose down -v

.PHONY: logs
logs: ## Follow the processor log
	docker compose logs -f processor

.PHONY: traffic
traffic: ## Continuous synthetic traffic at 200 events/s
	docker compose --profile traffic up -d producer

.PHONY: demo
demo: ## Run the four scenarios back to back
	docker compose run --rm setup orders-producer scenario clean --count 2000
	docker compose run --rm setup orders-producer scenario duplicates --count 2000
	docker compose run --rm setup orders-producer scenario late-burst --count 2000
	docker compose run --rm setup orders-producer scenario poison --count 500
	@echo "now run: make aggregates / make lateness / make dlq"

.PHONY: health
health: ## Pipeline health view
	docker compose run --rm setup orders-processor health

.PHONY: aggregates
aggregates: ## Most recent aggregate rows
	@$(PSQL) -c "SELECT window_start, merchant_id, events_total, orders_paid, \
	gross_amount_minor, distinct_users, late_events_applied, revision, is_closed \
	FROM agg_orders_minute ORDER BY window_start DESC, merchant_id LIMIT 20;"

.PHONY: lateness
lateness: ## Events that missed their window
	@$(PSQL) -c "SELECT merchant_id, window_start, round(lateness_seconds) AS late_s, recorded_at \
	FROM late_events ORDER BY recorded_at DESC LIMIT 20;"

.PHONY: dlq
dlq: ## Dead letters grouped by reason
	@$(PSQL) -c "SELECT reason, count(*), max(failed_at) AS newest FROM dlq_events GROUP BY 1 ORDER BY 2 DESC;"

.PHONY: dedup-proof
dedup-proof: ## Show that no event_id was applied twice
	@$(PSQL) -c "SELECT count(*) AS ledger_rows, count(DISTINCT event_id) AS distinct_ids FROM dedup_ledger;"

.PHONY: lag
lag: ## Consumer group lag
	docker compose exec -T redpanda rpk group describe orders-aggregator-v1
