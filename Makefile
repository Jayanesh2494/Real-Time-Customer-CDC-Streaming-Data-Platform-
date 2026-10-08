# Makefile for Real-Time Customer CDC & Streaming Data Platform

.PHONY: help up down restart ps setup topics register-cdc seed-data simulate-events test benchmark query logs

help:
	@echo "Available commands:"
	@echo "  make up               - Start all Docker containers in background"
	@echo "  make down             - Stop all Docker containers"
	@echo "  make restart          - Restart all containers"
	@echo "  make ps               - View container statuses"
	@echo "  make setup            - Initialize topics, register Debezium CDC, seed baseline data"
	@echo "  make seed-data        - Generate batch synthetic datasets"
	@echo "  make simulate-events  - Run live CDC event simulator (INSERT/UPDATE/DELETE/DLQ)"
	@echo "  make test             - Run pytest test suite"
	@echo "  make benchmark        - Run performance experiments (CSV vs Parquet, Partitioning)"
	@echo "  make query            - Run DuckDB analytical SQL models"
	@echo "  make logs             - Tail container logs"

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

ps:
	docker compose ps

setup:
	@echo "Provisioning Kafka topics..."
	python scripts/create_topics.py
	@echo "Registering Debezium PostgreSQL CDC connector..."
	python cdc/debezium/register_connector.py
	@echo "Seeding initial database records..."
	python scripts/generate_data.py --seed-db --export-local

seed-data:
	python scripts/generate_data.py --customers 500 --products 100 --orders 2000 --export-local

simulate-events:
	python scripts/generate_events.py --iterations 20 --delay 0.5

test:
	pytest tests/ -v

benchmark:
	python scripts/run_benchmarks.py

query:
	python analytics/duckdb_queries.py --query all

logs:
	docker compose logs -f
