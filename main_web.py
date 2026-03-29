from __future__ import annotations

import uvicorn

from bot.runtime import build_runtime
from web.app import create_app


def main() -> None:
    runtime = build_runtime(
        require_telegram_token=False,
        require_web_auth=True,
    )
    uvicorn.run(
        create_app(runtime=runtime),
        host=runtime.config.web_host,
        port=runtime.config.web_port,
    )


if __name__ == "__main__":
    main()
