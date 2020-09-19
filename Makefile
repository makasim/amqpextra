GOTEST?=go test

.PHONY: lint
## unit-test: run linter
lint:
	golangci-lint run --modules-download-mode=vendor --timeout=300s

.PHONY: unit-test
## unit-test: run unit  tests
unit-test:
	mockgen -source=publisher/interfaces.go > publisher/mock_publisher/mock_publisher.go
	$(GOTEST) -race -v -cover ./middleware/... ./publisher/...

.PHONY: e2e-test
## e2e-test: run end-to-end tests within docker with complete infrastructure
e2e-test:
	$(MAKE) -C test/e2e run

.PHONY: test
## test: run linter, unit and e2e tests
test:
	$(MAKE) lint
	$(MAKE) unit-test
	$(MAKE) e2e-test

.PHONY: help
## help: prints this help message
help:
	@echo "Usage: \n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'

.DEFAULT_GOAL := help
