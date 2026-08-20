BINARY := influx2tsdb-proxy
GO := go
LDFLAGS := -s -w
BUILD_DIR := build

.PHONY: all build clean linux amd64 arm64 darwin-amd64 darwin-arm64 windows cross run help

all: build

$(BUILD_DIR)/:
	@mkdir -p $(BUILD_DIR)

build: $(BUILD_DIR)/
	$(GO) build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY) .

linux: $(BUILD_DIR)/
	GOOS=linux GOARCH=amd64 $(GO) build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY)-linux-amd64 .

amd64: linux

arm64: $(BUILD_DIR)/
	GOOS=linux GOARCH=arm64 $(GO) build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY)-linux-arm64 .

darwin-amd64: $(BUILD_DIR)/
	GOOS=darwin GOARCH=amd64 $(GO) build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY)-darwin-amd64 .

darwin-arm64: $(BUILD_DIR)/
	GOOS=darwin GOARCH=arm64 $(GO) build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY)-darwin-arm64 .

windows: $(BUILD_DIR)/
	GOOS=windows GOARCH=amd64 $(GO) build -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY)-windows-amd64.exe .

cross: linux arm64 darwin-amd64 darwin-arm64 windows
	@echo "Cross-platform builds complete:"
	@ls -lh $(BUILD_DIR)/$(BINARY)-*

clean:
	rm -rf $(BUILD_DIR)

run: build
	./$(BUILD_DIR)/$(BINARY) -pg "$$PG_DSN"

help:
	@echo "Usage:"
	@echo "  make build          - Build for current platform -> $(BUILD_DIR)/$(BINARY)"
	@echo "  make linux          - Build for Linux amd64       -> $(BUILD_DIR)/$(BINARY)-linux-amd64"
	@echo "  make arm64          - Build for Linux arm64       -> $(BUILD_DIR)/$(BINARY)-linux-arm64"
	@echo "  make darwin-amd64   - Build for macOS Intel       -> $(BUILD_DIR)/$(BINARY)-darwin-amd64"
	@echo "  make darwin-arm64   - Build for macOS Apple Silicon -> $(BUILD_DIR)/$(BINARY)-darwin-arm64"
	@echo "  make windows        - Build for Windows amd64     -> $(BUILD_DIR)/$(BINARY)-windows-amd64.exe"
	@echo "  make cross          - Build all platforms"
	@echo "  make clean          - Remove $(BUILD_DIR)/ directory"
	@echo "  make run            - Build and run (local dev)"
