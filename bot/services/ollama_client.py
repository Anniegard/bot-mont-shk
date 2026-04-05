from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse

import aiohttp

logger = logging.getLogger(__name__)

_LOCAL_OLLAMA_HOSTS = {"127.0.0.1", "localhost", "::1"}


class OllamaError(Exception):
    pass


@dataclass(frozen=True)
class OllamaMessage:
    role: str
    content: str


def _validate_base_url(base_url: str) -> str:
    normalized = (base_url or "").strip().rstrip("/")
    if not normalized:
        raise OllamaError("OLLAMA_BASE_URL не настроен.")

    parsed = urlparse(normalized)
    if parsed.scheme not in {"http", "https"}:
        raise OllamaError("OLLAMA_BASE_URL должен начинаться с http:// или https://.")
    host = parsed.hostname or ""
    if host.lower() not in _LOCAL_OLLAMA_HOSTS:
        raise OllamaError(
            "OLLAMA_BASE_URL должен указывать на локальный Ollama "
            "(localhost, 127.0.0.1 или ::1)."
        )
    return normalized


class OllamaClient:
    def __init__(self, *, base_url: str, model: str | None, timeout_seconds: int) -> None:
        self.base_url = _validate_base_url(base_url)
        self.model = (model or "").strip()
        self.timeout_seconds = max(5, int(timeout_seconds))

    def ensure_configured(self) -> None:
        if not self.model:
            raise OllamaError("OLLAMA_MODEL не настроен.")

    def _base_payload(self, messages: Sequence[OllamaMessage]) -> dict[str, Any]:
        return {
            "model": self.model,
            "stream": False,
            "messages": [
                {"role": message.role, "content": message.content} for message in messages
            ],
            "options": {"temperature": 0.1},
        }

    async def _post_chat(self, payload: dict[str, Any]) -> dict[str, Any]:
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        url = f"{self.base_url}/api/chat"
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    url,
                    json=payload,
                    allow_redirects=False,
                ) as response:
                    raw_body = await response.text()
                    status = response.status
        except asyncio.TimeoutError as exc:
            raise OllamaError("Ollama не ответила вовремя. Попробуйте ещё раз.") from exc
        except aiohttp.ClientError as exc:
            raise OllamaError(
                "Не удалось подключиться к локальному Ollama. "
                "Проверьте, что сервис запущен."
            ) from exc

        if status >= 400:
            logger.warning("Ollama HTTP error status=%s body_prefix=%s", status, raw_body[:200])
            raise OllamaError(
                f"Ollama вернула HTTP {status}. Проверьте модель и доступность сервиса."
            )

        try:
            return json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise OllamaError("Ollama вернула некорректный JSON-ответ.") from exc

    async def chat(self, messages: Sequence[OllamaMessage]) -> str:
        self.ensure_configured()
        data = await self._post_chat(self._base_payload(messages))
        message = data.get("message") or {}
        content = message.get("content")
        if not isinstance(content, str) or not content.strip():
            raise OllamaError("Ollama вернула пустой ответ.")
        return content.strip()

    async def chat_json(
        self,
        messages: Sequence[OllamaMessage],
        *,
        json_schema: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Ask the model for JSON. Tries JSON schema `format` first (structured outputs),
        then falls back to format=\"json\" if the server rejects the request.
        """
        self.ensure_configured()
        base = self._base_payload(messages)
        last_error: OllamaError | None = None

        if json_schema is not None:
            payload = {**base, "format": dict(json_schema)}
            try:
                data = await self._post_chat(payload)
                return _parse_message_json_object(data)
            except OllamaError as exc:
                last_error = exc
                logger.info("Ollama structured format failed, trying json string mode: %s", exc)

        payload_json = {**base, "format": "json"}
        try:
            data = await self._post_chat(payload_json)
        except OllamaError:
            if last_error is not None:
                raise last_error from None
            raise
        return _parse_message_json_object(data)


def _parse_message_json_object(data: dict[str, Any]) -> dict[str, Any]:
    message = data.get("message") or {}
    content = message.get("content")
    if not isinstance(content, str) or not content.strip():
        raise OllamaError("Ollama вернула пустой JSON-ответ.")
    text = content.strip()
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise OllamaError("Ollama вернула текст, который не является JSON.") from exc
    if not isinstance(parsed, dict):
        raise OllamaError("Ollama вернула JSON не-объект.")
    return parsed
