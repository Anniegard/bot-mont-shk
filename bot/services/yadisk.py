from __future__ import annotations

import asyncio
from pathlib import Path
from urllib.parse import urlparse
from typing import Dict, Tuple

import logging

import aiohttp

logger = logging.getLogger(__name__)

# Cloud API (список файлов, ссылка на скачивание): конечные пределы, чтобы не зависать.
YADISK_CLOUD_API_TIMEOUT = aiohttp.ClientTimeout(
    total=120.0,
    connect=60.0,
    sock_connect=60.0,
    sock_read=90.0,
)
# Поток скачивания по href: долгий total, но ограничение на «молчащий» сокет.
YADISK_DOWNLOAD_STREAM_TIMEOUT = aiohttp.ClientTimeout(
    total=7200.0,
    connect=60.0,
    sock_connect=60.0,
    sock_read=600.0,
)


class YaDiskError(Exception):
    pass


_ALLOWED_YADISK_DOWNLOAD_HOSTS = ("yandex.net", "yandex.ru")


def _is_allowed_yadisk_download_href(href: str) -> bool:
    """
    Defense-in-depth for redirect/SSRF: allow only HTTPS URLs
    pointing to yandex.net / yandex.ru (including subdomains).
    """
    parsed = urlparse(href)
    if parsed.scheme != "https":
        return False
    host = (parsed.hostname or "").lower()
    if not host:
        return False
    for base_host in _ALLOWED_YADISK_DOWNLOAD_HOSTS:
        if host == base_host or host.endswith("." + base_host):
            return True
    return False


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
        raise YaDiskError(
            "Доступ запрещен к ресурсу Я.Диск (403). Проверьте токен/права."
        )  # noqa: TRY003
    if resp.status == 404:
        raise YaDiskError(
            "Ресурс Я.Диск не найден (404). Проверьте путь."
        )  # noqa: TRY003
    raise YaDiskError(f"Ошибка Яндекс.Диск {resp.status}: {text[:500]}")


async def yadisk_list_latest(
    token: str, folder_path: str, exts: Tuple[str, ...]
) -> Dict:
    folder_norm = _normalize_path(folder_path, ensure_dir=True)
    url = "https://cloud-api.yandex.net/v1/disk/resources"
    params = {"path": folder_norm, "limit": 50, "sort": "modified"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers=_auth_headers(token),
                params=params,
                timeout=YADISK_CLOUD_API_TIMEOUT,
            ) as resp:
                await _raise_for_status(resp)
                data = await resp.json()
    except asyncio.TimeoutError as exc:
        raise YaDiskError(
            "Таймаут при получении списка файлов с Яндекс.Диска. Проверьте сеть и повторите попытку."
        ) from exc
    except aiohttp.ClientError as exc:
        raise YaDiskError(
            "Сетевая ошибка при обращении к API Яндекс.Диска. Проверьте подключение."
        ) from exc
    embedded = data.get("_embedded", {})
    items = embedded.get("items", [])
    files = [
        item
        for item in items
        if item.get("type") == "file"
        and any(item.get("name", "").lower().endswith(ext.lower()) for ext in exts)
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


async def yadisk_list_files(
    token: str, folder_path: str, exts: Tuple[str, ...]
) -> list[Dict]:
    folder_norm = _normalize_path(folder_path, ensure_dir=True)
    url = "https://cloud-api.yandex.net/v1/disk/resources"
    offset = 0
    limit = 200
    items: list[Dict] = []

    try:
        async with aiohttp.ClientSession() as session:
            while True:
                params = {
                    "path": folder_norm,
                    "limit": limit,
                    "offset": offset,
                    "sort": "name",
                }
                async with session.get(
                    url,
                    headers=_auth_headers(token),
                    params=params,
                    timeout=YADISK_CLOUD_API_TIMEOUT,
                ) as resp:
                    await _raise_for_status(resp)
                    data = await resp.json()

                embedded = data.get("_embedded", {})
                page_items = embedded.get("items", [])
                if not page_items:
                    break

                items.extend(page_items)
                if len(page_items) < limit:
                    break
                offset += limit
    except asyncio.TimeoutError as exc:
        raise YaDiskError(
            "Таймаут при получении списка файлов с Яндекс.Диска. Проверьте сеть и повторите попытку."
        ) from exc
    except aiohttp.ClientError as exc:
        raise YaDiskError(
            "Сетевая ошибка при обращении к API Яндекс.Диска. Проверьте подключение."
        ) from exc

    files = [
        {
            "name": item.get("name"),
            "path": item.get("path"),
            "modified": item.get("modified"),
            "size": item.get("size"),
        }
        for item in items
        if item.get("type") == "file"
        and any(item.get("name", "").lower().endswith(ext.lower()) for ext in exts)
    ]

    if not files:
        raise YaDiskError("В папке нет подходящих файлов (xlsx/xls/zip).")

    return files


async def _download_stream(
    session: aiohttp.ClientSession, url: str, dest_path: Path, max_bytes: int
) -> int:
    downloaded = 0
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        async with session.get(url, timeout=YADISK_DOWNLOAD_STREAM_TIMEOUT) as resp:
            await _raise_for_status(resp)
            with dest_path.open("wb") as f:
                async for chunk in resp.content.iter_chunked(1024 * 64):
                    if chunk:
                        downloaded += len(chunk)
                        if downloaded > max_bytes:
                            raise YaDiskError(
                                "Скачивание прервано: файл превышает допустимый размер."
                            )
                        f.write(chunk)
    except asyncio.TimeoutError as exc:
        raise YaDiskError(
            "Таймаут при скачивании файла с Яндекс.Диска (долго нет данных по сети). "
            "Проверьте соединение или размер файла и повторите попытку."
        ) from exc
    except aiohttp.ClientError as exc:
        raise YaDiskError(
            "Сетевая ошибка при скачивании файла с Яндекс.Диска. Проверьте подключение."
        ) from exc
    return downloaded


async def yadisk_download_file(
    token: str, file_path: str, dest_path: str, max_bytes: int = 200 * 1024 * 1024
) -> Dict:
    file_norm = _normalize_path(file_path, ensure_dir=False)
    url = "https://cloud-api.yandex.net/v1/disk/resources/download"
    params = {"path": file_norm}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers=_auth_headers(token),
                params=params,
                timeout=YADISK_CLOUD_API_TIMEOUT,
            ) as resp:
                await _raise_for_status(resp)
                data = await resp.json()
            href = data.get("href")
            if not isinstance(href, str) or not href.strip():
                raise YaDiskError("Не удалось получить ссылку для скачивания файла.")
            href = href.strip()
            if not _is_allowed_yadisk_download_href(href):
                raise YaDiskError(
                    "Недопустимая ссылка для скачивания Я.Диска. Ожидается HTTPS на хостах "
                    "yandex.net/yandex.ru (включая поддомены)."
                )
            size = await _download_stream(session, href, Path(dest_path), max_bytes)
    except YaDiskError:
        raise
    except asyncio.TimeoutError as exc:
        raise YaDiskError(
            "Таймаут при запросе ссылки на скачивание с Яндекс.Диска. Проверьте сеть."
        ) from exc
    except aiohttp.ClientError as exc:
        raise YaDiskError(
            "Сетевая ошибка при обращении к API Яндекс.Диска. Проверьте подключение."
        ) from exc
    return {"path": dest_path, "size": size}
