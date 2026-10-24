"""Build guard regressions and a real local HTTP decompression smoke test."""

from importlib.metadata import PackageNotFoundError

import pytest

from scripts import check_brotli


@pytest.mark.parametrize("versions", [
    {},
    {"Brotli": "1.2.1"},
    {"brotlicffi": "1.2.1.0"},
    {"Brotli": "1.2.1", "brotlicffi": "1.2.1.0"},
    {"Brotli": "1.3.0", "brotlicffi": "1.3.0.0"},
])
def test_safe_or_absent_decoders(monkeypatch, versions):
    def version(name):
        if name not in versions:
            raise PackageNotFoundError(name)
        return versions[name]

    monkeypatch.setattr(check_brotli, "version", version)
    check_brotli.check_brotli()


@pytest.mark.parametrize("package,unsafe", [
    ("Brotli", "1.0.9"), ("Brotli", "1.1.0"), ("Brotli", "1.2.0"),
    ("brotlicffi", "1.2.0.2"), ("Brotli", "1.2.1rc1"),
    ("brotlicffi", "unknown"),
])
def test_unsafe_decoder_fails_even_with_other_decoder_patched(monkeypatch, package, unsafe):
    versions = {"Brotli": "1.2.1", "brotlicffi": "1.2.1.0", package: unsafe}
    monkeypatch.setattr(check_brotli, "version", versions.__getitem__)
    with pytest.raises(SystemExit, match="CVE-2026-111497"):
        check_brotli.check_brotli()


@pytest.mark.asyncio
async def test_aiohttp_brotli_response():
    """Exercise automatic decoding through the client used by backend and agent."""
    import aiohttp
    from aiohttp import web

    brotli = pytest.importorskip("brotli")
    check_brotli.check_brotli()
    payload = b'{"status":"ok"}'

    async def response(request):
        return web.Response(body=brotli.compress(payload), headers={"Content-Encoding": "br"})

    app = web.Application()
    app.router.add_get("/", response)
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}/") as result:
                assert await result.read() == payload
    finally:
        await runner.cleanup()
