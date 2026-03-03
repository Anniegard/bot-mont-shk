from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Dict, Tuple

import logging

import aiohttp
from aiohttp import ClientResponseError

logger = logging.getLogger(__name__)


class YaDiskError(Exception):
    pass


def _auth_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"OAuth {token}"}


def _normalize_path(path: str, ensure_dir: bool = False) -> str:
    """Ensure path starts with disk:/ and ends with / for folders."""
    p = path or ""
    if p.startswith("/"):
        p = "disk:" + p
    if not p.startswith("disk:"):
        p = "disk:/" + p.lstrip("/")
    if ensure_dir and not p.endswith("/"):
        p = p + "/"
    return p


async def _raise_for_status(resp: aiohttp.ClientResponse):
    if resp.status < 400:
        return
    try:
        text = await resp.text()
    except Exception:
        text = "<no text>"
    logger.error(
        "YaDisk HTTP error status=%s url=%s params=%s body=%s",
        resp.status,
        resp.url,
        dict(resp.request_info.real_url.query),
        text[:500],
    )
    if resp.status == 403:
        raise YaDiskError("Доступ запрещен к ресурсу Я.Диск (403). Проверьте токен/права.")  # noqa: TRY003
    if resp.status == 404:
        raise YaDiskError("Ресурс Я.Диск не найден (404). Проверьте путь.")  # noqa: TRY003
    raise YaDiskError(f"Ошибка Яндекс.Диск {resp.status}: {text[:500]}")


async def yadisk_list_latest(token: str, folder_path: str, exts: Tuple[str, ...]) -> Dict:
    folder_norm = _normalize_path(folder_path, ensure_dir=True)
    url = "https://cloud-api.yandex.net/v1/disk/resources"
    params = {"path": folder_norm, "limit": 50, "sort": "modified"}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=_auth_headers(token), params=params, timeout=15) as resp:
            await _raise_for_status(resp)
            data = await resp.json()
    embedded = data.get("_embedded", {})
    items = embedded.get("items", [])
    files = [
        item
        for item in items
        if item.get("type") == "file" and any(item.get("name", "").lower().endswith(ext.lower()) for ext in exts)
    ]
    if not files:
        raise YaDiskError("В папке нет подходящих файлов (xlsx/xls/zip).")
    # items already sorted by modified desc when sort=modified; take first
    latest = files[0]
    return {
        "name": latest.get("name"),
        "path": latest.get("path"),
        "modified": latest.get("modified"),
        "size": latest.get("size"),
    }


async def _download_stream(session: aiohttp.ClientSession, url: str, dest_path: Path, max_bytes: int) -> int:
    downloaded = 0
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=None)) as resp:
        await _raise_for_status(resp)
        with dest_path.open("wb") as f:
            async for chunk in resp.content.iter_chunked(1024 * 64):
                if chunk:
                    downloaded += len(chunk)
                    if downloaded > max_bytes:
                        raise YaDiskError("Скачивание прервано: файл превышает допустимый размер.")
                    f.write(chunk)
    return downloaded


async def yadisk_download_file(token: str, file_path: str, dest_path: str, max_bytes: int = 200 * 1024 * 1024) -> Dict:
    file_norm = _normalize_path(file_path, ensure_dir=False)
    url = "https://cloud-api.yandex.net/v1/disk/resources/download"
    params = {"path": file_norm}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=_auth_headers(token), params=params, timeout=15) as resp:
            await _raise_for_status(resp)
            data = await resp.json()
        href = data.get("href")
        if not href:
            raise YaDiskError("Не удалось получить ссылку для скачивания файла.")
        size = await _download_stream(session, href, Path(dest_path), max_bytes)
    return {"path": dest_path, "size": size}
