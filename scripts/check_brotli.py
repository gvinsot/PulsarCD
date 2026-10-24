"""Fail image builds containing unsafe optional Python Brotli decoders.

No decoder is required: aiohttp only enables Brotli when one is installed.
Check both distributions because clients differ in which one they prefer.
Debian's separately patched libbrotli1 is checked with dpkg in the Dockerfiles.
"""

import re
from importlib.metadata import PackageNotFoundError, version


MINIMUM_VERSIONS = {"Brotli": (1, 2, 1), "brotlicffi": (1, 2, 1, 0)}


def check_brotli():
    failures = []
    for package, minimum in MINIMUM_VERSIONS.items():
        try:
            installed = version(package)
        except PackageNotFoundError:
            print(f"{package}: absent (optional decoder disabled)")
            continue
        # Fail closed for prereleases/custom builds whose patch status is unknown.
        release = tuple(map(int, installed.split("."))) if re.fullmatch(r"\d+(?:\.\d+)*", installed) else ()
        if release < minimum:
            failures.append(f"{package}=={installed}: require >= {'.'.join(map(str, minimum))}")
        else:
            print(f"{package}=={installed}: patched")
    if failures:
        raise SystemExit("CVE-2026-111497: " + "; ".join(failures))


if __name__ == "__main__":
    check_brotli()
