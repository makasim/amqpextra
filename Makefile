GOTEST?=go test
RUNTEST?=".*"

.PHONY: lint
## unit-test: run linter
lint:
	golangci-lint run --modules-download-mode=vendor --timeout=300s

.PHONY: unit-test
## unit-test: run unit  tests
unit-test:
ifndef NOMOCKGEN
	mockgen github.com/makasim/amqpextra/publisher AMQPConnection,AMQPChannel > publisher/mock_publisher/mocks.go
	mockgen github.com/makasim/amqpextra/consumer AMQPConnection,AMQPChannel > consumer/mock_consumer/mocks.go
	mockgen github.com/makasim/amqpextra AMQPConnection > mock_amqpextra/mocks.go
endif
	
	$(GOTEST) -race -v -cover -run $(RUNTEST) ./ ./publisher/... ./consumer/...

.PHONY: e2e-test
## e2e-test: run end-to-end tests within docker with complete infrastructure
e2e-test:
	$(MAKE) -C e2e_test run

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
