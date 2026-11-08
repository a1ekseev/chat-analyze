version := env("VERSION", "dev")
commit := `git rev-parse --short HEAD 2>/dev/null || echo none`

default:
    just --list

fmt:
    golangci-lint fmt

lint:
    golangci-lint run --build-tags=integration

test:
    go test -race -count=1 ./...

test-integration:
    #!/usr/bin/env bash
    set -u
    rc=0
    docker compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from tests || rc=$?
    docker compose -f docker-compose.test.yml down -v
    exit $rc

generate:
    go generate ./...

build:
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version={{version}} -X main.commit={{commit}}" -o bin/chat-analyze ./cmd/chat-analyze

up:
    VERSION={{version}} COMMIT={{commit}} docker compose up -d --build

down:
    docker compose down

login:
    docker compose run --rm login

migrate *args="up":
    docker compose run --rm migrate migrate {{args}}

vuln:
    go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...

ci: lint test vuln test-integration
