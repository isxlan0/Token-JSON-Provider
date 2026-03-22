#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

mkdir -p bin
rm -f bin/token-atlas bin/token-atlas.exe
rm -rf bin/dist

build_target() {
    local goos="$1"
    local goarch="$2"
    local output_name="$3"
    GOOS="$goos" GOARCH="$goarch" go build -o "$DIR/bin/$output_name" ./cmd/server
}

build_target windows amd64 token-atlas-windows-amd64.exe
build_target windows arm64 token-atlas-windows-arm64.exe
build_target linux amd64 token-atlas-linux-amd64
build_target linux arm64 token-atlas-linux-arm64
build_target darwin amd64 token-atlas-macos-amd64
build_target darwin arm64 token-atlas-macos-arm64

echo "Built platform binaries in $DIR/bin"
