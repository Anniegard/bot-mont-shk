from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi.testclient import TestClient
from starlette.requests import Request

from bot.config import Config, load_config
from bot.runtime import AppRuntime
from bot.services.processing import WorkflowOutcome
from web.app import create_app
from web.security import client_ip


class FakeProcessingService:
    def __init__(self, temp_dir: Path) -> None:
        self.lock = asyncio.Lock()
        self.temp_dir = temp_dir
        self.calls: list[tuple[str, str, str | None, str | None]] = []

    def make_temp_path(self, prefix: str, suffix: str) -> Path:
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        return self.temp_dir / f"{prefix}{suffix}"

    @asynccontextmanager
    async def processing_slot(self):
        async with self.lock:
            yield

    async def process_local_source(
        self,
        expected: str,
        file_path: str | Path,
        file_info,
        *,
        no_move_export_mode: str | None = None,
    ) -> WorkflowOutcome:
        self.calls.append((expected, "file", no_move_export_mode, Path(file_path).name))
        return WorkflowOutcome(
            title="Test local",
            message="Локальный файл обработан.",
            payload={"filename": file_info.filename},
        )

    async def process_latest_yadisk_file(
        self,
        expected: str,
        *,
        no_move_export_mode: str | None = None,
    ) -> WorkflowOutcome:
        self.calls.append((expected, "yadisk_latest", no_move_export_mode, None))
        return WorkflowOutcome(title="Test latest", message="Я.Диск обработан.")

    async def process_warehouse_delay_multiple(self) -> WorkflowOutcome:
        self.calls.append(("warehouse_delay_multiple", "yadisk_folder", None, None))
        return WorkflowOutcome(
            title="Test folder",
            message="Папка задержки склада обработана.",
        )


def make_runtime(
    tmp_path: Path,
    *,
    web_trust_proxy_headers: bool = False,
    web_rate_limit_per_minute: int = 20,
) -> AppRuntime:
    config = Config(
        telegram_token=None,
        spreadsheet_id="spreadsheet-id",
        google_credentials_path=tmp_path / "credentials.json",
        db_path=tmp_path / "bot.db",
        public_base_url="http://testserver",
        web_secret_key="secret-key",
        web_rate_limit_per_minute=web_rate_limit_per_minute,
        web_trust_proxy_headers=web_trust_proxy_headers,
        web_admin_username="boss",
        web_admin_password="pass123",
        web_user_username="webuser",
        web_user_password="userpass",
    )
    return AppRuntime(config=config, db_path=config.db_path, gspread_client=object())


def test_dashboard_redirects_without_login(tmp_path: Path) -> None:
    app = create_app(
        runtime=make_runtime(tmp_path),
        processing_service=FakeProcessingService(tmp_path / "tmp"),
    )
    client = TestClient(app)

    response = client.get("/app", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/login"


def test_login_and_file_processing_flow(tmp_path: Path) -> None:
    service = FakeProcessingService(tmp_path / "tmp")
    app = create_app(runtime=make_runtime(tmp_path), processing_service=service)
    client = TestClient(app)
    login_page = client.get("/login")
    csrf_token = login_page.text.split('name="csrf_token" value="', 1)[1].split('"', 1)[0]

    login_response = client.post(
        "/login",
        data={
            "username": "boss",
            "password": "pass123",
            "csrf_token": csrf_token,
        },
        follow_redirects=False,
    )
    assert login_response.status_code == 303
    assert login_response.headers["location"] == "/app"

    dashboard_response = client.get("/app")
    assert dashboard_response.status_code == 200
    assert "Защищённое приложение" in dashboard_response.text
    dashboard_csrf = dashboard_response.text.split('name="csrf_token" value="', 1)[1].split(
        '"',
        1,
    )[0]

    response = client.post(
        "/app/actions/process",
        data={
            "workflow": "no_move",
            "source_kind": "file",
            "export_mode": "export_with_transfers",
            "csrf_token": dashboard_csrf,
        },
        files={"upload": ("no_move.xlsx", b"fake-binary", "application/octet-stream")},
    )

    assert response.status_code == 200
    assert "Локальный файл обработан." in response.text
    assert service.calls == [
        ("no_move", "file", "export_with_transfers", "web_upload.xlsx")
    ]


def test_load_config_supports_web_only_runtime(tmp_path: Path, monkeypatch) -> None:
    credentials_path = tmp_path / "credentials.json"
    credentials_path.write_text("{}", encoding="utf-8")

    monkeypatch.delenv("TELEGRAM_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("BOT_CONFIG_FILE", raising=False)
    monkeypatch.setenv("SPREADSHEET_ID", "spreadsheet-id")
    monkeypatch.setenv("GOOGLE_CREDENTIALS_PATH", str(credentials_path))
    monkeypatch.setenv("WEB_SECRET_KEY", "secret-key")
    monkeypatch.setenv("WEB_ADMIN_USERNAME", "boss")
    monkeypatch.setenv("WEB_ADMIN_PASSWORD", "pass123")
    monkeypatch.setenv("WEB_USER_USERNAME", "webuser")
    monkeypatch.setenv("WEB_USER_PASSWORD", "userpass")

    config = load_config(
        env_path=str(tmp_path / "isolated.env"),
        require_telegram_token=False,
        require_web_auth=True,
    )

    assert config.telegram_token in {None, ""}
    assert config.web_secret_key == "secret-key"
    assert config.web_admin_username == "boss"
    assert config.web_user_username == "webuser"


def test_web_user_cannot_access_admin_page(tmp_path: Path) -> None:
    app = create_app(
        runtime=make_runtime(tmp_path),
        processing_service=FakeProcessingService(tmp_path / "tmp"),
    )
    client = TestClient(app)
    login_page = client.get("/login")
    csrf_token = login_page.text.split('name="csrf_token" value="', 1)[1].split('"', 1)[0]

    client.post(
        "/login",
        data={
            "username": "webuser",
            "password": "userpass",
            "csrf_token": csrf_token,
        },
        follow_redirects=False,
    )

    response = client.get("/app/admin")
    assert response.status_code == 403
    assert "Нет прав администратора" in response.text


def test_web_user_can_use_dashboard(tmp_path: Path) -> None:
    app = create_app(
        runtime=make_runtime(tmp_path),
        processing_service=FakeProcessingService(tmp_path / "tmp"),
    )
    client = TestClient(app)
    login_page = client.get("/login")
    csrf_token = login_page.text.split('name="csrf_token" value="', 1)[1].split('"', 1)[0]

    client.post(
        "/login",
        data={
            "username": "webuser",
            "password": "userpass",
            "csrf_token": csrf_token,
        },
    )

    dashboard = client.get("/app")
    assert dashboard.status_code == 200
    assert "Защищённое приложение" in dashboard.text


def test_admin_can_access_admin_page(tmp_path: Path) -> None:
    app = create_app(
        runtime=make_runtime(tmp_path),
        processing_service=FakeProcessingService(tmp_path / "tmp"),
    )
    client = TestClient(app)
    login_page = client.get("/login")
    csrf_token = login_page.text.split('name="csrf_token" value="', 1)[1].split('"', 1)[0]

    client.post(
        "/login",
        data={
            "username": "boss",
            "password": "pass123",
            "csrf_token": csrf_token,
        },
    )

    response = client.get("/app/admin")
    assert response.status_code == 200
    assert "Защищённая административная зона" in response.text
    assert "boss" in response.text


def test_login_requires_valid_csrf(tmp_path: Path) -> None:
    app = create_app(
        runtime=make_runtime(tmp_path),
        processing_service=FakeProcessingService(tmp_path / "tmp"),
    )
    client = TestClient(app)

    client.get("/login")
    response = client.post(
        "/login",
        data={"username": "boss", "password": "pass123", "csrf_token": "bad-token"},
        follow_redirects=False,
    )

    assert response.status_code == 400


def test_process_action_requires_valid_csrf(tmp_path: Path) -> None:
    service = FakeProcessingService(tmp_path / "tmp")
    app = create_app(runtime=make_runtime(tmp_path), processing_service=service)
    client = TestClient(app)

    login_page = client.get("/login")
    assert login_page.status_code == 200
    csrf_token = login_page.text.split('name="csrf_token" value="', 1)[1].split('"', 1)[0]
    client.post(
        "/login",
        data={"username": "boss", "password": "pass123", "csrf_token": csrf_token},
    )

    response = client.post(
        "/app/actions/process",
        data={
            "workflow": "h24",
            "source_kind": "yadisk_latest",
            "csrf_token": "bad-token",
        },
    )

    assert response.status_code == 400


def test_client_ip_ignores_proxy_headers_when_trust_disabled(monkeypatch) -> None:
    monkeypatch.setenv("WEB_TRUST_PROXY_HEADERS", "false")
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [
                (b"x-forwarded-for", b"198.51.100.10, 203.0.113.7"),
                (b"x-real-ip", b"203.0.113.7"),
            ],
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
            "scheme": "http",
            "query_string": b"",
        }
    )

    assert client_ip(request) == "127.0.0.1"


def test_client_ip_uses_proxy_headers_when_trust_enabled(monkeypatch) -> None:
    monkeypatch.setenv("WEB_TRUST_PROXY_HEADERS", "true")
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [
                (b"x-forwarded-for", b"198.51.100.10, 203.0.113.7"),
                (b"x-real-ip", b"203.0.113.7"),
            ],
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
            "scheme": "http",
            "query_string": b"",
        }
    )

    assert client_ip(request) == "203.0.113.7"


def test_rate_limit_ignores_proxy_headers_when_trust_disabled(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("WEB_TRUST_PROXY_HEADERS", "false")
    service = FakeProcessingService(tmp_path / "tmp")
    app = create_app(
        runtime=make_runtime(
            tmp_path,
            web_rate_limit_per_minute=1,
            web_trust_proxy_headers=False,
        ),
        processing_service=service,
    )
    client = TestClient(app)

    login_page = client.get("/login")
    assert login_page.status_code == 200
    csrf_token = login_page.text.split('name="csrf_token" value="', 1)[1].split('"', 1)[0]

    headers1 = {
        "x-real-ip": "203.0.113.7",
        "x-forwarded-for": "198.51.100.10, 203.0.113.7",
    }
    resp1 = client.post(
        "/login",
        data={"username": "webuser", "password": "bad", "csrf_token": csrf_token},
        headers=headers1,
        follow_redirects=False,
    )
    assert resp1.status_code != 429

    headers2 = {
        "x-real-ip": "203.0.113.8",
        "x-forwarded-for": "198.51.100.11, 203.0.113.8",
    }
    resp2 = client.post(
        "/login",
        data={"username": "webuser", "password": "bad", "csrf_token": csrf_token},
        headers=headers2,
        follow_redirects=False,
    )
    assert resp2.status_code == 429


def test_rate_limit_respects_proxy_headers_when_trust_enabled(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("WEB_TRUST_PROXY_HEADERS", "true")
    service = FakeProcessingService(tmp_path / "tmp")
    app = create_app(
        runtime=make_runtime(
            tmp_path,
            web_rate_limit_per_minute=1,
            web_trust_proxy_headers=True,
        ),
        processing_service=service,
    )
    client = TestClient(app)

    login_page = client.get("/login")
    assert login_page.status_code == 200
    csrf_token = login_page.text.split('name="csrf_token" value="', 1)[1].split('"', 1)[0]

    headers1 = {"x-real-ip": "203.0.113.7"}
    resp1 = client.post(
        "/login",
        data={"username": "webuser", "password": "bad", "csrf_token": csrf_token},
        headers=headers1,
        follow_redirects=False,
    )
    assert resp1.status_code != 429

    headers2 = {"x-real-ip": "203.0.113.8"}
    resp2 = client.post(
        "/login",
        data={"username": "webuser", "password": "bad", "csrf_token": csrf_token},
        headers=headers2,
        follow_redirects=False,
    )
    assert resp2.status_code != 429
