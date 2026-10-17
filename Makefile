REDIS_URL   ?= redis://localhost:6379/0
ADMIN_TOKEN ?= local-dev-admin-token-change-me
BASE_URLS   ?= http://localhost:8080

export REDIS_URL

.PHONY: help redis test stress lint fmt up down logs e2e load run

help: ## Show targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-8s %s\n", $$1, $$2}'

redis: ## Start a throwaway Redis on :6379 for tests
	@docker start rl-test-redis >/dev/null 2>&1 || docker run -d --name rl-test-redis -p 6379:6379 redis:7-alpine >/dev/null
	@echo "redis on $(REDIS_URL)"

test: ## Unit, contract, HTTP and multi-instance tests with the race detector
	go test -race -count=1 ./...

stress: ## Repeat the concurrency tests
	go test -race -count=20 -run 'Concurrent|Contention|MultiInstance|Exact' ./limiter/... ./test/...

lint: ## go vet + golangci-lint
	go vet -tags e2e ./...
	golangci-lint run ./...

fmt: ## Format code
	golangci-lint fmt ./...

run: ## Run one instance locally against $(REDIS_URL)
	ADMIN_TOKEN=$(ADMIN_TOKEN) go run .

up: ## Two instances + Redis behind nginx on :8080
	ADMIN_TOKEN=$(ADMIN_TOKEN) docker compose up --build -d

down: ## Stop the compose stack (keeps the Redis volume)
	docker compose down

logs: ## Tail instance logs
	docker compose logs -f app1 app2

e2e: ## End-to-end tests against $(BASE_URLS)
	E2E_BASE_URLS=$(BASE_URLS) E2E_ADMIN_TOKEN=$(ADMIN_TOKEN) E2E_FRESH_CONNS=true E2E_MIN_INSTANCES=2 \
		go test -tags e2e -count=1 -v ./test/e2e/

load: ## Cross-instance load test against $(BASE_URLS)
	go run ./tools/loadtest -urls $(BASE_URLS) -token $(ADMIN_TOKEN) -n 5000 -c 200 -burst 100 -fresh-conns -min-instances 2
