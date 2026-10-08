<#
.SYNOPSIS
    Real-Time Customer CDC & Streaming Platform - PowerShell CLI Automation

.DESCRIPTION
    Convenient single-command executor for Windows environments.

.EXAMPLE
    .\run.ps1 up
    .\run.ps1 test
    .\run.ps1 benchmark
    .\run.ps1 query
#>

param(
    [Parameter(Position=0)]
    [ValidateSet("up", "down", "restart", "ps", "setup", "seed", "simulate", "test", "benchmark", "query", "logs", "help")]
    [string]$Action = "help"
)

switch ($Action) {
    "up" {
        Write-Host "Starting all platform containers..." -ForegroundColor Cyan
        docker compose up -d
        docker compose ps
    }
    "down" {
        Write-Host "Stopping all platform containers..." -ForegroundColor Yellow
        docker compose down
    }
    "restart" {
        Write-Host "Restarting platform containers..." -ForegroundColor Yellow
        docker compose restart
    }
    "ps" {
        docker compose ps
    }
    "setup" {
        Write-Host "Provisioning MinIO Data Lake buckets..." -ForegroundColor Cyan
        python scripts\setup_minio.py
        Write-Host "Provisioning Kafka topics..." -ForegroundColor Cyan
        python scripts\create_topics.py
        Write-Host "Registering Debezium CDC Connector..." -ForegroundColor Cyan
        python cdc\debezium\register_connector.py
        Write-Host "Seeding baseline operational datasets..." -ForegroundColor Cyan
        python scripts\generate_data.py --seed-db --export-local
    }
    "seed" {
        Write-Host "Generating batch test data..." -ForegroundColor Cyan
        python scripts\generate_data.py --customers 200 --products 50 --orders 1000 --export-local
    }
    "simulate" {
        Write-Host "Launching live CDC event simulator..." -ForegroundColor Cyan
        python scripts\generate_events.py --iterations 20 --delay 0.5
    }
    "simulate-dlq" {
        Write-Host "Injecting poison pill records into Dead-Letter Queue (DLQ)..." -ForegroundColor Cyan
        python scripts\inject_dlq_errors.py
    }
    "test" {
        Write-Host "Executing pytest test suite..." -ForegroundColor Cyan
        pytest tests\ -v
    }
    "benchmark" {
        Write-Host "Running performance benchmarks..." -ForegroundColor Cyan
        python scripts\run_benchmarks.py
    }
    "query" {
        Write-Host "Executing DuckDB analytical models..." -ForegroundColor Cyan
        python analytics\duckdb_queries.py --query all
    }
    "logs" {
        docker compose logs -f
    }
    Default {
        Write-Host "=== Real-Time CDC & Streaming Platform CLI ===" -ForegroundColor Green
        Write-Host "Usage: .\run.ps1 [command]"
        Write-Host "  up        : Start all Docker containers"
        Write-Host "  down      : Stop all Docker containers"
        Write-Host "  ps        : View container status"
        Write-Host "  setup     : Provision topics, register CDC, seed data"
        Write-Host "  seed      : Generate synthetic batch dataset"
        Write-Host "  simulate  : Run live CDC event simulator"
        Write-Host "  test      : Run automated test suite"
        Write-Host "  benchmark : Run empirical performance experiments"
        Write-Host "  query     : Execute DuckDB analytical SQL models"
        Write-Host "  logs      : Tail container logs"
    }
}
