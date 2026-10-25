# initialize project environment
.PHONY: setup
setup:
	@sh ./scripts/setup_env.sh
	@sh ./scripts/setup_hook.sh

# format code
.PHONY: fmt
fmt:
	@goimports -l -w $$(find . -type f -name '*.go' -not -path "./.idea/*" -not -path "./.vscode/*")
	@gofumpt -l -w $$(find . -type f -name '*.go' -not -path "./.idea/*" -not -path "./.vscode/*")

# clean up project dependencies
.PHONY: tidy
tidy:
	@go mod tidy -v

# check code format and dependencies
.PHONY: check
check:
	@$(MAKE) --no-print-directory fmt
	@$(MAKE) --no-print-directory tidy

# static check with go vet
.PHONY: vet
vet:
	@go vet ./...

# lint code (baseline tier, CI blocking)
.PHONY: lint
lint:
	@golangci-lint run -c ./scripts/lint/.golangci.yaml ./...

# lint code (strict tier, report-only target)
.PHONY: lint-strict
lint-strict:
	@golangci-lint run -c ./scripts/lint/.golangci.strict.yaml ./...

# build binary with version metadata injected
.PHONY: build
build:
	@VERSION=$$(git describe --tags --always --dirty 2>/dev/null || echo dev); \
	COMMIT=$$(git rev-parse --short HEAD 2>/dev/null || echo none); \
	DATE=$$(date -u '+%Y-%m-%dT%H:%M:%SZ'); \
	go build -trimpath \
		-ldflags "-s -w \
			-X 'github.com/jrmarcco/synp/internal/pkg/buildinfo.Version=$${VERSION}' \
			-X 'github.com/jrmarcco/synp/internal/pkg/buildinfo.Commit=$${COMMIT}' \
			-X 'github.com/jrmarcco/synp/internal/pkg/buildinfo.Date=$${DATE}'" \
		-o bin/synp ./cmd

# build container image
# 国内网络: make image GOPROXY=https://goproxy.cn,direct
.PHONY: image
image:
	@VERSION=$$(git describe --tags --always --dirty 2>/dev/null || echo dev); \
	COMMIT=$$(git rev-parse --short HEAD 2>/dev/null || echo none); \
	DATE=$$(date -u '+%Y-%m-%dT%H:%M:%SZ'); \
	docker build -f ./scripts/build/Dockerfile \
		--build-arg VERSION="$${VERSION}" \
		--build-arg COMMIT="$${COMMIT}" \
		--build-arg DATE="$${DATE}" \
		$(if $(GOPROXY),--build-arg GOPROXY=$(GOPROXY)) \
		-t synp:"$${VERSION}" .
