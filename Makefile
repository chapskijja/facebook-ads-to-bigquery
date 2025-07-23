.PHONY: build up down daily backfill status logs shell clean test

# Build the Docker image
build:
	docker-compose build

# Run daily sync
daily:
	docker-compose up facebook-ads-etl

# Run backfill for 365 days
backfill:
	docker-compose --profile backfill up facebook-ads-backfill

# Run backfill for custom days (usage: make backfill-days DAYS=90)
backfill-days:
	docker-compose run --rm facebook-ads-etl python run_etl.py backfill --days $(DAYS)

# Check ETL status
status:
	docker-compose run --rm facebook-ads-etl python run_etl.py status

# Test conversions
test:
	docker-compose run --rm facebook-ads-etl python test_conversions.py

# View logs
logs:
	docker-compose logs facebook-ads-etl

# Get shell access to container
shell:
	docker-compose run --rm facebook-ads-etl bash

# Start in background
up:
	docker-compose up -d facebook-ads-etl

# Stop containers
down:
	docker-compose down

# Clean up everything
clean:
	docker-compose down --rmi all --volumes

# Setup environment (create directories)
setup:
	mkdir -p secret logs
	@echo "📝 Create your .env file with Facebook API and Google Cloud credentials"
	@echo "📂 Copy your Google Cloud service account JSON to secret/ directory"

# Show help
help:
	@echo "🐳 Facebook Ads to BigQuery ETL - Docker Commands"
	@echo ""
	@echo "📋 Available commands:"
	@echo "  make setup         - Create directories and show setup instructions"
	@echo "  make build         - Build Docker image"
	@echo "  make daily         - Run daily sync"
	@echo "  make backfill      - Run backfill for 365 days"
	@echo "  make backfill-days DAYS=90 - Run backfill for custom days"
	@echo "  make status        - Check ETL status"
	@echo "  make test          - Test Facebook API connection"
	@echo "  make up            - Start in background"
	@echo "  make down          - Stop containers"
	@echo "  make logs          - View logs"
	@echo "  make shell         - Get bash shell in container"
	@echo "  make clean         - Clean up everything"
	@echo ""

# Default target
default: help 