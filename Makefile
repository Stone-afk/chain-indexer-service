E2E_COMPOSE := docker compose -p chain-indexer-service-e2e -f scripts/test_docker_compose.yml

.PHONY: e2e
e2e:
	@set -eu; \
	cleanup() { $(E2E_COMPOSE) down -v --remove-orphans; }; \
	on_exit() { \
		status=$$?; \
		cleanup_status=0; \
		cleanup || cleanup_status=$$?; \
		if [ "$$status" -ne 0 ]; then exit "$$status"; fi; \
		exit "$$cleanup_status"; \
	}; \
	trap on_exit EXIT; \
	trap 'exit 130' INT; \
	trap 'exit 143' TERM; \
	cleanup; \
	$(E2E_COMPOSE) up -d --wait; \
	go test -race -shuffle=on -failfast -tags=e2e ./internal/test/integration/...
