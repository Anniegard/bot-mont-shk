from __future__ import annotations

import logging
from pathlib import Path

from telegram.ext import Application

from bot.config import load_config
from bot.db import init_db
from bot.handlers import BotHandlers
from bot.logging_config import setup_logging
from bot.services.sheets import authorize_client


def main() -> None:
    config = load_config()
    setup_logging(config.telegram_token)
    db_path = init_db(config.db_path)

    gclient = authorize_client(config.google_credentials_path)

    application = Application.builder().token(config.telegram_token).build()
    application.bot_data["workdir"] = Path(__file__).resolve().parent
    application.bot_data["db_path"] = db_path

    handlers = BotHandlers(config, gclient)
    handlers.register(application)

    logger = logging.getLogger(__name__)
    logger.info("SQLite initialized at %s", db_path)
    logger.info("Бот запущен. Polling начат.")
    application.run_polling()


if __name__ == "__main__":
    main()
