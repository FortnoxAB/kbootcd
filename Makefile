.PHONY:	build test
SHELL := /bin/bash
MAKEFLAGS += --no-print-directory

fix:
	go fix ./...

build:
	CGO_ENABLED=0 GOOS=linux go build

localrun:
	go run main.go

generate:
	go generate ./...

test:
	go test ./... -count=1 -cover

container:
ifndef TAG
	$(error TAG is required. Use: make container TAG=<tag>)
endif
	$(shell command -v podman &>/dev/null && echo "podman" || echo "docker") build -t $(TAG) -f Containerfile .
