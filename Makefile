run:
	docker compose -f compose.yaml up -d --wait
	uv run python -m scripts.init_raw_data

sync-raw:
	uv run python -m scripts.sync_raw_layer

build-marts:
	uv run python -m scripts.build_dwh_layers

pipeline:
	uv run python -m scripts.sync_raw_layer
	uv run python -m scripts.build_dwh_layers

down:
	docker compose -f compose.yaml down

init:
	uv sync