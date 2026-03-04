#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_DIR="$ROOT_DIR/dist"
PKG_NAME="aidb"
ARCH="amd64"
VERSION=$(awk -F'"' '/^version =/ {print $2}' "$ROOT_DIR/Cargo.toml")
BUILD_DIR="$DIST_DIR/${PKG_NAME}_${VERSION}_${ARCH}"

mkdir -p "$DIST_DIR"

pushd "$ROOT_DIR" >/dev/null
cargo build --release
popd >/dev/null

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR/DEBIAN"
mkdir -p "$BUILD_DIR/usr/bin"
mkdir -p "$BUILD_DIR/lib/systemd/system"
mkdir -p "$BUILD_DIR/var/lib/aidb"
mkdir -p "$BUILD_DIR/var/log/aidb"

install -m 0755 "$ROOT_DIR/target/release/my_ai_db" "$BUILD_DIR/usr/bin/aidb-server"
install -m 0755 "$ROOT_DIR/scripts/packaging/aidb-uninstall" "$BUILD_DIR/usr/bin/aidb-uninstall"
install -m 0644 "$ROOT_DIR/scripts/packaging/aidb.service" "$BUILD_DIR/lib/systemd/system/aidb.service"

cat <<EOF > "$BUILD_DIR/DEBIAN/control"
Package: aidb
Version: ${VERSION}
Section: database
Priority: optional
Architecture: ${ARCH}
Maintainer: aiDB Maintainers <maintainers@aidb.local>
Depends: libc6, libssl3
Description: aiDB hybrid vector database server
 A hybrid vector database with REST, gRPC, SQL, NoSQL, and RAG support.
EOF

install -m 0755 "$ROOT_DIR/scripts/packaging/postinst" "$BUILD_DIR/DEBIAN/postinst"
install -m 0755 "$ROOT_DIR/scripts/packaging/prerm" "$BUILD_DIR/DEBIAN/prerm"
install -m 0755 "$ROOT_DIR/scripts/packaging/postrm" "$BUILD_DIR/DEBIAN/postrm"

chmod 0755 "$BUILD_DIR/DEBIAN"

pushd "$DIST_DIR" >/dev/null
dpkg-deb --build "${PKG_NAME}_${VERSION}_${ARCH}"
popd >/dev/null

echo "Deb package created: $DIST_DIR/${PKG_NAME}_${VERSION}_${ARCH}.deb"
