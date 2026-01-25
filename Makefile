SHELL := /bin/bash

.PHONY: build build-core build-core-shared build-cli clean install

GO ?= go
GOOS ?= $(shell $(GO) env GOOS)
GOARCH ?= $(shell $(GO) env GOARCH)

BUILD_DIR := build
CLI_BIN := $(BUILD_DIR)/ledgerdb
CORE_LIB := $(BUILD_DIR)/libledgerdb.a
CORE_SO := $(BUILD_DIR)/libledgerdb.so
CORE_DYLIB := $(BUILD_DIR)/libledgerdb.dylib

CORE_PKG := ./pkg/ledgerdb
CLI_PKG := ./cmd/ledgerdb

LINK_SHARED ?= 0
ifeq ($(GOOS),darwin)
  ifeq ($(GOARCH),arm64)
    PREFIX ?= /opt/homebrew
  else
    PREFIX ?= /usr/local
  endif
else
  PREFIX ?= /usr/local
endif
BINDIR ?= $(PREFIX)/bin
LIBDIR ?= $(PREFIX)/lib
INSTALL ?= install
SUDO ?= sudo

build: build-core build-cli

build-core: | build-dir
	$(GO) build -buildmode=archive -o $(CORE_LIB) $(CORE_PKG)

build-core-shared: | build-dir
	@set -e; \
	if [ "$(GOOS)" = "darwin" ] && [ "$(GOARCH)" = "arm64" ]; then \
		echo "buildmode=shared not supported on $(GOOS)/$(GOARCH); falling back to archive"; \
		$(MAKE) build-core; \
	else \
		if $(GO) build -buildmode=shared $(CORE_PKG); then \
			if [ -f libledgerdb.dylib ]; then \
				mv libledgerdb.dylib $(CORE_DYLIB); \
			elif [ -f libledgerdb.so ]; then \
				mv libledgerdb.so $(CORE_SO); \
			else \
				echo "shared library not produced"; \
				exit 1; \
			fi; \
		else \
			echo "buildmode=shared failed; falling back to archive"; \
			$(MAKE) build-core; \
		fi; \
	fi

build-cli: | build-dir
	@flags=""; \
	if [ "$(LINK_SHARED)" = "1" ]; then flags="$$flags -linkshared"; fi; \
	$(GO) build $$flags -o $(CLI_BIN) $(CLI_PKG)

build-dir:
	mkdir -p $(BUILD_DIR)

clean:
	rm -rf $(BUILD_DIR)

install: build-core-shared build-cli
	@set -euo pipefail; \
	if [ "$(GOOS)" = "windows" ]; then \
		echo "install not supported on windows; copy $(CLI_BIN) manually"; \
		exit 1; \
	fi; \
	sudo_cmd=""; \
	needs_sudo=0; \
	for dir in "$(BINDIR)" "$(LIBDIR)"; do \
		if [ -d "$$dir" ]; then \
			[ -w "$$dir" ] || needs_sudo=1; \
		else \
			parent=$$(dirname "$$dir"); \
			[ -w "$$parent" ] || needs_sudo=1; \
		fi; \
	done; \
	if [ "$$needs_sudo" -eq 1 ]; then \
		if [ -z "$(SUDO)" ]; then \
			echo "error: install needs elevated privileges; set SUDO=sudo or use writable PREFIX"; \
			exit 1; \
		fi; \
		sudo_cmd="$(SUDO)"; \
	fi; \
	$$sudo_cmd $(INSTALL) -d "$(BINDIR)" "$(LIBDIR)"; \
	$$sudo_cmd $(INSTALL) -m 0755 $(CLI_BIN) "$(BINDIR)/ledgerdb"; \
	if [ -f "$(CORE_SO)" ]; then \
		$$sudo_cmd $(INSTALL) -m 0644 $(CORE_SO) "$(LIBDIR)/"; \
	elif [ -f "$(CORE_DYLIB)" ]; then \
		$$sudo_cmd $(INSTALL) -m 0644 $(CORE_DYLIB) "$(LIBDIR)/"; \
	elif [ -f "$(CORE_LIB)" ]; then \
		$$sudo_cmd $(INSTALL) -m 0644 $(CORE_LIB) "$(LIBDIR)/"; \
	else \
		echo "no core library found under $(BUILD_DIR)"; \
		exit 1; \
	fi; \
	case ":$$PATH:" in \
		*:"$(BINDIR)":*) ;; \
		*) echo "warning: $(BINDIR) is not on PATH; add it to your shell profile" ;; \
	esac
