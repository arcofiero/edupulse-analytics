.PHONY: up down logs test simulate simulate-backfill pipeline-local quality-local lint clean

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

test:
	pytest tests/ -v

simulate:
	python simulator/main.py --mode=live

simulate-backfill:
	python simulator/main.py --mode=backfill --weeks=16

pipeline-local:
	python -m pipeline.local --clean --weeks=1 --students=50 --seed=42 --limit=500

quality-local:
	python -m soda.local --data-dir .local/lakehouse --layer all

lint:
	ruff check .

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
