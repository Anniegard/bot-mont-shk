from __future__ import annotations

import logging
from pathlib import Path

from telegram.ext import Application

from bot.config import load_config
from bot.handlers import BotHandlers
from bot.logging_config import setup_logging
from bot.services.sheets import authorize_client


def main() -> None:
    config = load_config()
    setup_logging(config.telegram_token)

    gclient = authorize_client(config.google_credentials_path)

    application = Application.builder().token(config.telegram_token).build()
    application.bot_data["workdir"] = Path(__file__).resolve().parent

    handlers = BotHandlers(config, gclient)
    handlers.register(application)

    logging.getLogger(__name__).info("Бот запущен. Polling начат.")
    application.run_polling()


if __name__ == "__main__":
    main()
