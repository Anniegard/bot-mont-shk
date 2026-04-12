from __future__ import annotations

import asyncio
from pathlib import Path

from bot.services import yadisk as yadisk_module
from bot.services.yadisk import YaDiskError, _download_stream, yadisk_download_file


class FakeResponse:
    def __init__(self, href: str):
        self.status = 200
        self._href = href

    async def __aenter__(self) -> "FakeResponse":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def json(self) -> dict[str, str]:
        return {"href": self._href}


class FakeClientSession:
    def __init__(self, href: str):
        self._href = href

    async def __aenter__(self) -> "FakeClientSession":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    def get(self, *_args: object, **_kwargs: object) -> FakeResponse:
        return FakeResponse(self._href)


def test_yadisk_download_file_valid_href(tmp_path: Path, monkeypatch) -> None:
    href = "https://download.yandex.net/some/path/file.zip"
    dest_path = tmp_path / "out.zip"

    async def fake_download_stream(_session, url: str, _dest_path: Path, _max_bytes: int) -> int:
        assert url == href
        return 123

    monkeypatch.setattr(yadisk_module.aiohttp, "ClientSession", lambda: FakeClientSession(href))
    monkeypatch.setattr(yadisk_module, "_download_stream", fake_download_stream)

    result = asyncio.run(
        yadisk_download_file(
            token="token",
            file_path="disk:/some/file.zip",
            dest_path=str(dest_path),
            max_bytes=1024 * 1024,
        )
    )
    assert result["path"] == str(dest_path)
    assert result["size"] == 123


def test_yadisk_download_file_rejects_invalid_href(tmp_path: Path, monkeypatch) -> None:
    invalid_hrefs = [
        "http://yandex.net/some/path/file.zip",
        "https://evil.com/some/path/file.zip",
        "https://yandex.com/some/path/file.zip",
        "yandex.net/some/path/file.zip",
    ]

    for href in invalid_hrefs:
        dest_path = tmp_path / f"out_{abs(hash(href))}.zip"

        async def fake_download_stream(*_args: object, **_kwargs: object) -> int:  # pragma: no cover
            raise AssertionError("Download stream must not be called for invalid href")

        monkeypatch.setattr(
            yadisk_module.aiohttp,
            "ClientSession",
            lambda href=href: FakeClientSession(href),
        )
        monkeypatch.setattr(yadisk_module, "_download_stream", fake_download_stream)

        try:
            asyncio.run(
                yadisk_download_file(
                    token="token",
                    file_path="disk:/some/file.zip",
                    dest_path=str(dest_path),
                    max_bytes=1024 * 1024,
                )
            )
        except YaDiskError as exc:
            assert "Недопустимая ссылка" in str(exc)
        else:
            raise AssertionError("Expected YaDiskError for invalid href")


def test_download_stream_wraps_timeout_error_from_reader(tmp_path: Path) -> None:
    """aiohttp поднимает asyncio.TimeoutError при таймауте чтения; оборачиваем в YaDiskError."""

    class FakeResp:
        status = 200

        async def __aenter__(self) -> FakeResp:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        class _Content:
            async def iter_chunked(self, _n: int):
                raise asyncio.TimeoutError()
                yield b""  # pragma: no cover

        content = _Content()

    class FakeSession:
        def get(self, *_a: object, **_k: object) -> FakeResp:
            return FakeResp()

    dest = tmp_path / "out.bin"

    async def run() -> None:
        await _download_stream(FakeSession(), "https://download.yandex.net/x", dest, 10**9)

    try:
        asyncio.run(run())
    except YaDiskError as exc:
        assert "Таймаут при скачивании" in str(exc)
    else:
        raise AssertionError("Expected YaDiskError when stream read times out")

