.PHONY: up deps seed run test build docs serve lint check verify down
up:
	docker compose up -d --build
deps:
	docker compose run --rm dbt dbt deps
seed:
	docker compose run --rm dbt dbt seed
run:
	docker compose run --rm dbt dbt run
test:
	docker compose run --rm dbt dbt test
build:
	docker compose run --rm dbt dbt build
docs:
	docker compose run --rm dbt dbt docs generate
serve:
	docker compose --profile docs up -d docs
lint:
	docker compose run --rm -w /workspace dbt sqlfluff lint dbt/models dbt/tests
check: deps build lint docs
verify:
	docker compose run --rm -w /workspace dbt python scripts/run_checks.py
down:
	docker compose down
