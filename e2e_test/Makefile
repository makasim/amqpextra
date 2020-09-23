APP?=amqpextra
export COMPOSE_PROJECT_NAME?=$(APP)-e2e-test
TEST?=./e2e_test/...
GOTEST?=go test

.PHONY: build-image
build-image:
	docker-compose --file sandbox/docker-compose.yml build

.PHONY: run
run: clean
	go mod tidy
	go mod vendor
	docker-compose --file sandbox/docker-compose.yml run $(APP)-e2e-test

.PHONY: clean
clean:
	docker-compose --file sandbox/docker-compose.yml down --remove-orphans
	docker-compose --file sandbox/docker-compose.yml down --volumes

.PHONY: enter
enter: clean
	docker-compose --file sandbox/docker-compose.yml run --entrypoint=bash $(APP)-e2e-test

.PHONY: test
## test: should be used inside sandbox to run e2e tests
## test: could be used with make enter
test:
	cd ../; $(GOTEST) -mod=vendor -race -count=1  -v $(TEST)
	
.PHONY: help
## help: prints this help message
help:
	@echo "Usage: \n"
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' |  sed -e 's/^/ /'

.DEFAULT_GOAL := help