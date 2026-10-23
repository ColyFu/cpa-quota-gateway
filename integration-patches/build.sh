#!/usr/bin/env bash
# Rebuild the optional Keeper console and trusted CPA routing binaries.
# Requires Git, Node.js 22.22+ and Go with automatic toolchain downloads enabled.
set -euo pipefail
REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_ROOT="${1:-$REPO_ROOT/.integration-build}"
mkdir -p "$BUILD_ROOT"
BUILD_ROOT="$(cd -- "$BUILD_ROOT" && pwd)"

fetch_source() {
  local name="$1" repository="$2" revision="$3"
  if [[ -e "$BUILD_ROOT/$name" ]]; then
    echo "Refusing to overwrite existing source: $BUILD_ROOT/$name" >&2
    exit 1
  fi
  git init -q "$BUILD_ROOT/$name"
  git -C "$BUILD_ROOT/$name" remote add origin "$repository"
  git -C "$BUILD_ROOT/$name" fetch --depth=1 origin "$revision"
  git -C "$BUILD_ROOT/$name" checkout --detach FETCH_HEAD
  git -C "$BUILD_ROOT/$name" apply "$REPO_ROOT/integration-patches/$name.patch"
}

fetch_source keeper https://github.com/Willxup/cpa-usage-keeper.git d62cad3f345ae574089a14a4ac75cca023c7ead6
fetch_source cpa-reference https://github.com/router-for-me/CLIProxyAPI.git 091ff27f31285ec4fd1dd5823b44f7f51fb8f48b
(cd "$BUILD_ROOT/keeper/web" && npm ci --registry=https://registry.npmjs.org && npm run typecheck && npm run build)
mkdir -p "$BUILD_ROOT/release"
go -C "$BUILD_ROOT/keeper" build -o "$BUILD_ROOT/release/cpa-usage-keeper" ./cmd/server
go -C "$BUILD_ROOT/cpa-reference" build -o "$BUILD_ROOT/release/cli-proxy-api" ./cmd/server
cp -a "$BUILD_ROOT/keeper/web/dist" "$BUILD_ROOT/release/console"
echo "Built artifacts in $BUILD_ROOT/release. Production services were not changed."
