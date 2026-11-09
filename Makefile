# controller-crashtest. `make help` lists the targets.

BIN    := bin/crashtest
KTHENA ?= ../kthena
REF    ?= main
CRD    := charts/kthena/charts/workload/crds/workload.serving.volcano.sh_modelservings.yaml

.PHONY: help build run list test lint crd kthena-local kthena-pinned update-kthena clean

help: ## show this list
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-15s %s\n", $$1, $$2}'

build: ## build bin/crashtest
	go build -o $(BIN) ./cmd/crashtest

run: build ## run every check on every scenario (pass flags with ARGS="-scenario create")
	./$(BIN) run $(ARGS)

list: build ## list targets, scenarios and checks
	./$(BIN) list

test: ## crashtest's own tests (a toy controller with planted bugs)
	go test ./...

lint: ## gofmt and go vet
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)
	go vet ./...

crd: ## refresh the ModelServing CRD copy from the kthena version in use
	cp "$$(go list -m -f '{{.Dir}}' github.com/volcano-sh/kthena)/$(CRD)" targets/modelserving/modelserving-crd.yaml
	chmod 644 targets/modelserving/modelserving-crd.yaml

kthena-local: ## test a local kthena checkout instead (make kthena-local KTHENA=../kthena)
	go work init . $(KTHENA)
	$(MAKE) crd

kthena-pinned: ## go back to the kthena version pinned in go.mod
	rm -f go.work go.work.sum
	$(MAKE) crd

update-kthena: ## pin a newer kthena (make update-kthena REF=main)
	go get github.com/volcano-sh/kthena@$(REF)
	go mod tidy
	$(MAKE) crd

clean: ## remove the binary and old results
	rm -rf bin results
