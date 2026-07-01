#!/usr/bin/env bash
# Linux build/test harness for nats.swift.
#
# Runs `swift build` and `swift test` inside a production Swift toolchain image
# (swift:6.2-noble), with a real `nats-server` binary on PATH so the integration
# suite actually executes (rather than skipping). This is the canonical "does it
# work on Linux" gate.
#
# Usage:
#   scripts/linux-test.sh             # build + test
#   scripts/linux-test.sh build       # build only
#   scripts/linux-test.sh test        # test only
#   NATS_SERVER_VERSION=2.10.22 scripts/linux-test.sh
#
# A separate scratch dir (.build-linux) keeps Linux artifacts from clashing with
# the host's macOS .build.
set -euo pipefail

MODE="${1:-all}"
IMAGE="swift:6.2-noble"
NATS_SERVER_VERSION="${NATS_SERVER_VERSION:-2.10.22}"
PKG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# --no-parallel: the integration suites each spawn a nats-server; running suites
# concurrently causes server-port contention/deadlock on Linux. Serial execution
# runs the whole suite with no hangs.
case "$MODE" in
  build) SWIFT_CMDS='swift build --scratch-path .build-linux' ;;
  test)  SWIFT_CMDS='swift test --scratch-path .build-linux --no-parallel' ;;
  all)   SWIFT_CMDS='swift build --scratch-path .build-linux && swift test --scratch-path .build-linux --no-parallel' ;;
  *) echo "unknown mode: $MODE (use build|test|all)" >&2; exit 2 ;;
esac

exec docker run --rm -t \
  -v "$PKG_DIR":/pkg -w /pkg \
  -e NATS_SERVER_VERSION="$NATS_SERVER_VERSION" \
  "$IMAGE" bash -euo pipefail -c '
    arch="$(dpkg --print-architecture)"   # amd64 | arm64
    case "$arch" in
      amd64) nats_arch=amd64 ;;
      arm64) nats_arch=arm64 ;;
      *) echo "unsupported arch: $arch" >&2; exit 1 ;;
    esac
    url="https://github.com/nats-io/nats-server/releases/download/v${NATS_SERVER_VERSION}/nats-server-v${NATS_SERVER_VERSION}-linux-${nats_arch}.tar.gz"
    echo "Installing nats-server v${NATS_SERVER_VERSION} (${nats_arch}) ..."
    # No system crypto C libraries are required: TLS is served by swift-nio-ssl
    # (BoringSSL built from source) and the remaining crypto by swift-crypto, so
    # only curl/ca-certificates are installed to fetch nats-server.
    apt-get update -qq && apt-get install -y -qq curl ca-certificates >/dev/null
    curl -sSL "$url" -o /tmp/nats.tgz
    tar -xzf /tmp/nats.tgz -C /tmp
    install -m 0755 "/tmp/nats-server-v${NATS_SERVER_VERSION}-linux-${nats_arch}/nats-server" /usr/local/bin/nats-server
    nats-server --version
    echo "=== swift ==="; swift --version
    echo "=== running: '"$SWIFT_CMDS"' ==="
    '"$SWIFT_CMDS"'
  '
