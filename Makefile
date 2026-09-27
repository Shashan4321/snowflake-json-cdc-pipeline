.PHONY: data local test lint
data:    ## regenerate sample_data/*.json (seeded)
	PYTHONPATH=src python -m sfcdc.events
local:   ## run the CDC logic on DuckDB, batch by batch
	PYTHONPATH=src python -m sfcdc.local_pipeline
test:
	pytest -q
lint:
	ruff check . && ruff format --check . && sqlfluff lint local_sql/
