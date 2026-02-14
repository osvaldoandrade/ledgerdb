#!/bin/sh
set -eu

LEDGERDB_REPO_URL="${LEDGERDB_REPO_URL:-https://github.com/osvaldoandrade/ledgerdb.git}"
LEDGERDB_REF="${LEDGERDB_REF:-main}" # branch, tag, or commit sha
LEDGERDB_BIN_NAME="${LEDGERDB_BIN_NAME:-}" # defaults to detected ./cmd/<name>
LEDGERDB_PKG="${LEDGERDB_PKG:-}"          # Go package (defaults to auto-detect from ./cmd)
LEDGERDB_BIN_DIR="${LEDGERDB_BIN_DIR:-}"  # install dir (defaults to first writable dir on PATH)

say() { printf '%s\n' "$*"; }
warn() { printf '%s\n' "warning: $*" >&2; }
die() { printf '%s\n' "error: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "missing dependency: $1"; }

uname_s="$(uname -s 2>/dev/null || printf unknown)"
os="unknown"
case "$uname_s" in
	Darwin*) os="darwin" ;;
	Linux*) os="linux" ;;
	MINGW* | MSYS* | CYGWIN*) os="windows" ;;
esac

exe=""
if [ "$os" = "windows" ]; then
	exe=".exe"
fi

find_writable_path_dir() {
	old_ifs="$IFS"
	IFS=":"
	for p in $PATH; do
		[ -n "${p:-}" ] || continue
		[ "$p" = "." ] && continue
		if [ -d "$p" ] && [ -w "$p" ]; then
			IFS="$old_ifs"
			printf '%s' "$p"
			return 0
		fi
	done
	IFS="$old_ifs"
	return 1
}

bin_dir="$LEDGERDB_BIN_DIR"
if [ -z "$bin_dir" ]; then
	if bin_dir="$(find_writable_path_dir 2>/dev/null)"; then
		:
	else
		if [ "$os" = "windows" ]; then
			bin_dir="${HOME}/bin"
		else
			bin_dir="${HOME}/.local/bin"
		fi
	fi
fi

mkdir -p "$bin_dir"

need go
need git

tmp_dir="$(mktemp -d 2>/dev/null || mktemp -d -t ledgerdb-install)"
cleanup() { rm -rf "$tmp_dir"; }
trap cleanup EXIT INT TERM

src_dir="$tmp_dir/src"
mkdir -p "$src_dir"

say "Installing ${LEDGERDB_BIN_NAME:-ledgerdb}${exe} (${LEDGERDB_REF}) from ${LEDGERDB_REPO_URL}"

git init -q "$src_dir"
git -C "$src_dir" remote add origin "$LEDGERDB_REPO_URL"
git -C "$src_dir" fetch -q --depth 1 origin "$LEDGERDB_REF"
git -C "$src_dir" checkout -q FETCH_HEAD

detect_pkg() {
	# Prefer the canonical cmd name if present.
	if [ -d "./cmd/ledgerdb" ] && ls "./cmd/ledgerdb"/*.go >/dev/null 2>&1; then
		printf '%s' "./cmd/ledgerdb"
		return 0
	fi

	if [ ! -d "./cmd" ]; then
		return 1
	fi

	count=0
	chosen=""
	for d in ./cmd/*; do
		[ -d "$d" ] || continue
		ls "$d"/*.go >/dev/null 2>&1 || continue
		if grep -qs "^package main" "$d"/*.go 2>/dev/null && grep -qs "func main" "$d"/*.go 2>/dev/null; then
			count=$((count + 1))
			chosen="$d"
		fi
	done

	if [ "$count" -eq 1 ]; then
		printf '%s' "$chosen"
		return 0
	fi
	return 1
}

(
	cd "$src_dir"

	pkg="$LEDGERDB_PKG"
	if [ -z "$pkg" ]; then
		if pkg="$(detect_pkg 2>/dev/null)"; then
			:
		else
			die "failed to auto-detect CLI under ./cmd; set LEDGERDB_PKG (e.g. ./cmd/ledgerdb)"
		fi
	fi

	bin_name="$LEDGERDB_BIN_NAME"
	if [ -z "$bin_name" ]; then
		# Default to ./cmd/<name>
		bin_name="$(basename "$pkg")"
	fi

	out_path="$tmp_dir/${bin_name}${exe}"
	CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o "$out_path" "$pkg"

	dest_path="${bin_dir%/}/${bin_name}${exe}"
	if command -v install >/dev/null 2>&1; then
		install -m 0755 "$out_path" "$dest_path"
	else
		cp "$out_path" "$dest_path"
		chmod 0755 "$dest_path" 2>/dev/null || true
	fi

	say "Installed to: $dest_path"
	case ":$PATH:" in
		*":$bin_dir:"*) ;;
		*)
			warn "install dir is not on PATH: $bin_dir"
			if [ "$os" = "windows" ]; then
				warn "add it to PATH (Git Bash): export PATH=\"$bin_dir:\$PATH\" (persist in ~/.bashrc)"
			else
				warn "add it to PATH (bash/zsh): export PATH=\"$bin_dir:\$PATH\" (persist in ~/.profile or ~/.zshrc)"
			fi
			;;
	esac
)

