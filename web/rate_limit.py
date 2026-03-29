from __future__ import annotations

from collections import defaultdict, deque
from time import monotonic

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import HTMLResponse, PlainTextResponse, Response

from web.security import client_ip


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, *, requests_per_minute: int) -> None:
        super().__init__(app)
        self.requests_per_minute = max(requests_per_minute, 1)
        self._buckets: dict[str, deque[float]] = defaultdict(deque)

    async def dispatch(self, request: Request, call_next) -> Response:
        if not self._should_limit(request):
            return await call_next(request)

        client_host = client_ip(request)
        now = monotonic()
        bucket = self._buckets[client_host]
        while bucket and now - bucket[0] >= 60:
            bucket.popleft()

        if len(bucket) >= self.requests_per_minute:
            if request.headers.get("HX-Request") == "true":
                return HTMLResponse(
                    (
                        '<section class="panel-card border border-amber-400/30">'
                        '<div class="panel-label">Rate limit</div>'
                        '<h2 class="mt-3 text-2xl font-semibold text-white">Слишком много запросов</h2>'
                        '<div class="mt-4 whitespace-pre-line leading-7 text-slate-200">'
                        "Подождите минуту и повторите действие."
                        "</div></section>"
                    ),
                    status_code=429,
                )
            return PlainTextResponse(
                "Слишком много запросов. Подождите минуту и повторите.",
                status_code=429,
            )

        bucket.append(now)
        return await call_next(request)

    def _should_limit(self, request: Request) -> bool:
        if request.method != "POST":
            return False
        return request.url.path in {"/login", "/app/actions/process"}
