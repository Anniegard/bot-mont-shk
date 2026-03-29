from __future__ import annotations

import logging
from pathlib import Path

from telegram.ext import Application

from bot.constants import APP_VERSION
from bot.handlers import BotHandlers
from bot.runtime import build_runtime
from bot.services.case_sync import sync_cases_from_master_sheet


def main() -> None:
    runtime = build_runtime(require_telegram_token=True)
    config = runtime.config
    db_path = runtime.db_path
    gclient = runtime.gspread_client
    logger = logging.getLogger(__name__)
    logger.info("Bot_Mont_SHK v%s", APP_VERSION)

    try:
        sync_summary = sync_cases_from_master_sheet(
            client=gclient,
            spreadsheet_id=config.spreadsheet_id,
            db_path=db_path,
        )
        logger.info(
            "Master sheet sync complete: rows=%s writes=%s case_id_updates=%s",
            sync_summary["rows_read"],
            sync_summary["rows_written"],
            sync_summary["case_id_updates"],
        )
    except Exception:
        logger.exception("Master sheet sync failed during startup")

    application = Application.builder().token(config.telegram_token).build()
    application.bot_data["workdir"] = Path(__file__).resolve().parent
    application.bot_data["db_path"] = db_path

    handlers = BotHandlers(config, gclient)
    handlers.register(application)

    logger.info("SQLite initialized at %s", db_path)
    logger.info("Бот запущен. Polling начат.")
    application.run_polling()


if __name__ == "__main__":
    main()
