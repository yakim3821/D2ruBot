param(
    [switch]$Build
)

$ErrorActionPreference = "Stop"

if ($Build) {
    docker compose up -d --build postgres
} else {
    docker compose up -d postgres
}

docker compose ps postgres
