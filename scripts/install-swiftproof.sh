#!/usr/bin/env bash
# Run on the Linux deployment host using a reviewed copy of this script.
set -euo pipefail
readonly version=v0.2.0
case "$(uname -m)" in
    x86_64) arch=amd64; sha=fce4fdf345bf36ead40b8b6c232b4dcd3c40106a186ed350434755d20a0ef6d1 ;;
    aarch64|arm64) arch=arm64; sha=ba2302e778ce157dfb5497b2e73f722a5cde91b14ff68237337a90fbe388e3d1 ;;
    *) echo 'SwiftProof installer supports Linux amd64 and arm64.' >&2; exit 1 ;;
esac
[[ "$(uname -s)" == Linux ]] || { echo 'Linux required.' >&2; exit 1; }
destination="${1:-/usr/local/bin/swiftproof}"
temporary=$(mktemp -d)
trap 'rm -rf -- "$temporary"' EXIT
archive="swiftproof-$version-linux-$arch.tar.gz"
curl --fail --location --retry 3 "https://github.com/gvinsot/SwiftProof/releases/download/$version/$archive" -o "$temporary/$archive"
(cd "$temporary" && printf '%s  %s\n' "$sha" "$archive" | sha256sum --check)
tar -xzf "$temporary/$archive" -C "$temporary"
install -m 0755 "$temporary/swiftproof-$version-linux-$arch/swiftproof" "$destination"
"$destination" version
