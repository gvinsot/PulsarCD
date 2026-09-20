#!/usr/bin/env bash
# Run on the Linux deployment host using a reviewed copy of this script.
set -euo pipefail
case "$(uname -m)" in
    x86_64) arch=amd64; sha=27f7fdf8fceccae0f8fd2b7df2e6d67ebecb094afe12d26a923977f38290099d ;;
    aarch64|arm64) arch=arm64; sha=334f4e017d6408e5adab1dad83dc99520810581c274844cd52ea3092c2efcfd8 ;;
    *) echo 'SwiftProof installer supports Linux amd64 and arm64.' >&2; exit 1 ;;
esac
[[ "$(uname -s)" == Linux ]] || { echo 'Linux required.' >&2; exit 1; }
destination="${1:-/usr/local/bin/swiftproof}"
temporary=$(mktemp -d)
trap 'rm -rf -- "$temporary"' EXIT
archive="swiftproof-v0.1.0-linux-$arch.tar.gz"
curl --fail --location --retry 3 "https://github.com/gvinsot/SwiftProof/releases/download/v0.1.0/$archive" -o "$temporary/$archive"
(cd "$temporary" && printf '%s  %s\n' "$sha" "$archive" | sha256sum --check)
tar -xzf "$temporary/$archive" -C "$temporary"
install -m 0755 "$temporary/swiftproof-v0.1.0-linux-$arch/swiftproof" "$destination"
"$destination" version
