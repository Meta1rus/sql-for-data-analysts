run:
	docker compose -f compose.yaml up -d --wait
	uv run python -m scripts.init_raw_data

sync-raw:
	uv run python -m scripts.sync_raw_layer

down:
	docker compose -f compose.yaml down

init:
	uv sync