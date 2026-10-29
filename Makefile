run:
	docker compose -f compose.yaml up -d --wait
	uv run python -m scripts.init_raw_data

down:
	docker compose -f compose.yaml down

init:
	uv sync