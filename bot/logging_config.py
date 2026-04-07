from __future__ import annotations

import logging
import re
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Iterable


class _TokenRedactor(logging.Filter):
    def __init__(self, secrets: Iterable[str | None]):
        super().__init__()
        self.patterns: list[str] = []
        for secret in secrets:
            if not secret:
                continue
            normalized = str(secret).strip()
            if not normalized:
                continue
            self.patterns.extend([normalized, f"bot{normalized}", re.escape(normalized)])

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        message = record.getMessage()
        redacted = message
        for pattern in self.patterns:
            redacted = redacted.replace(pattern, "[REDACTED]")
        record.msg = redacted
        record.args = ()
        return True


def setup_logging(*secrets_to_redact: str | None) -> None:
    root_dir = Path(__file__).resolve().parent.parent
    log_dir = root_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "bot.log"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S"
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    if secrets_to_redact:
        redactor = _TokenRedactor(secrets_to_redact)
        stream_handler.addFilter(redactor)
        file_handler.addFilter(redactor)

    logger.handlers.clear()
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
