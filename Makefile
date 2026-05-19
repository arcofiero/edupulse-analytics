.PHONY: up down logs test simulate lint clean

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

lint:
	ruff check .

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
