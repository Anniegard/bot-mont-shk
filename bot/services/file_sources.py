from __future__ import annotations

import asyncio
import zipfile
from pathlib import Path
from typing import Literal, Tuple

import aiohttp
from aiohttp import ClientResponseError

YA_PUBLIC_PREFIXES = ("https://disk.yandex.ru/d/", "https://yadi.sk/d/")
DIRECT_EXTENSIONS = (".xlsx", ".xls", ".zip")


def is_url(text: str) -> bool:
    return text.strip().lower().startswith(("http://", "https://"))


def detect_source(url: str) -> Literal["yandex_disk_public", "direct", "unknown"]:
    lower = url.lower()
    if lower.startswith(YA_PUBLIC_PREFIXES):
        return "yandex_disk_public"
    if lower.endswith(DIRECT_EXTENSIONS):
        return "direct"
    return "unknown"


async def _fetch_json(
    session: aiohttp.ClientSession, url: str, retries: int = 3, timeout: int = 10
):
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=timeout) as resp:
                resp.raise_for_status()
                return await resp.json()
        except Exception:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(1)


async def _download_stream(
    session: aiohttp.ClientSession, url: str, dest_path: Path, max_bytes: int
) -> int:
    downloaded = 0
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=None)) as resp:
            resp.raise_for_status()
            with dest_path.open("wb") as f:
                async for chunk in resp.content.iter_chunked(1024 * 64):
                    if chunk:
                        downloaded += len(chunk)
                        if downloaded > max_bytes:
                            raise ValueError(
                                "Скачивание прервано: файл превышает допустимый размер."
                            )
                        f.write(chunk)
    except ClientResponseError as e:
        if "captcha" in str(e).lower():
            raise ValueError(
                "Яндекс.Диск вернул капчу/ограничение по ссылке. Откройте ссылку в браузере, убедитесь что доступ открыт "
                "для всех и попробуйте снова (можно пересоздать публичную ссылку)."
            ) from e
        raise ValueError(f"Не удалось скачать файл: {e.status} {e.message}") from e
    return downloaded


async def download_from_url(
    url: str, dest_path: str, max_bytes: int = 200 * 1024 * 1024
) -> Tuple[str, int, str]:
    """Скачать файл по URL. Возвращает (путь, размер_байт, source_type)."""
    source = detect_source(url)
    dest = Path(dest_path)
    async with aiohttp.ClientSession() as session:
        target_url = url
        if source == "yandex_disk_public":
            api_url = (
                "https://cloud-api.yandex.net/v1/disk/public/resources/download?public_key="
                + aiohttp.helpers.quote(url, safe="")
            )
            data = await _fetch_json(session, api_url)
            target_url = data.get("href")
            if not target_url:
                raise ValueError("Не удалось получить ссылку скачивания Яндекс.Диск.")
        elif source == "unknown":
            source = "direct"

        size = await _download_stream(session, target_url, dest, max_bytes)
        return str(dest), size, source


def maybe_extract_zip(path: str, workdir: str) -> str:
    p = Path(path)
    if p.suffix.lower() != ".zip":
        return str(p)

    extract_dir = Path(workdir) / (p.stem + "_unzipped")
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(p, "r") as zf:
        members = zf.namelist()
        excel_members = [m for m in members if m.lower().endswith((".xlsx", ".xls"))]
        if not excel_members:
            raise ValueError("В архиве нет Excel файлов.")
        target_member = excel_members[0]
        zf.extract(target_member, path=extract_dir)
        extracted_path = extract_dir / target_member
        return str(extracted_path)
