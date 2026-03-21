$ErrorActionPreference = "Stop"

docker compose down -v
docker compose up -d postgres
docker compose ps postgres
