from __future__ import annotations

import uvicorn

from bot.config import load_config
from web.app import create_app

app = create_app()


def main() -> None:
    config = load_config(require_telegram_token=False)
    uvicorn.run(
        "web_main:app",
        host=config.web_host,
        port=config.web_port,
        reload=False,
    )


if __name__ == "__main__":
    main()
