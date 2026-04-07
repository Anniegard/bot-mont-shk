# Changelog

## [1.2.0] — 2026-04-07

### Добавлено

- В Telegram-бот добавлен admin-only AI-режим на OpenAI: команды `/ai`, `/ai_use`, `/ai_reset`, `/ai_exit`, загрузка AI-источников через Telegram и использование проектных источников `no_move`, `24h`, `warehouse_delay`.
- Добавлен отдельный AI-сервисный слой (`bot/services/ai/`) с provider abstraction, `OpenAIProvider`, deterministic source loading и bounded context building.
- Добавлены AI feature flags и лимиты (`AI_*`, `OPENAI_*`) для контролируемого rollout без включения по умолчанию.

### Изменено

- Логирование расширено на redaction нескольких секретов и key=value observability для AI-запросов.
- `handlers.py` интегрирован с AI-контроллером без изменения web-потока и без переписывания существующих Excel workflow.

## [1.1.2] — 2026-04-07

### Изменено

- Telegram-бот переведён на webhook-режим (с fallback на polling): добавлены `TELEGRAM_WEBHOOK_*` переменные, запуск через `run_webhook(...)` при наличии `TELEGRAM_WEBHOOK_URL`.
- Обновлены deploy-шаблоны для VM: nginx-маршрут `/tg/webhook/` на порт бота и systemd-параметр `PYTHONUNBUFFERED=1`.
- README дополнен чеклистом безопасного переключения (`deleteWebhook` → `setWebhook` → `getWebhookInfo` + smoke-тест).

## [1.1.1] — 2026-03-30

### Изменено

- В web-интерфейсе добавлен визуальный индикатор обработки (load-bar) во время HTMX-запросов к `/app/actions/process`, чтобы пользователь видел, что обработка идёт.
- Исправлено мобильное отображение кнопок в блоке «Без движения»: корректная высота/перенос текста без выхода надписи за фон.

## [1.1.0] — 2026-03-30

Веб-интерфейс и рефакторинг обработки.

### Добавлено

- Веб-приложение **AnniLand** на FastAPI (`web/`, точки входа `main_web.py`, `web_main.py`): сессии, CSRF, rate limit на логин и обработку, статика и шаблоны.
- Слой `bot/runtime.py`, сервисы `bot/services/processing.py` и `bot/services/workflows.py` для общей логики Telegram и web.
- Примеры деплоя: `deploy/` (nginx, systemd).
- Тесты: `tests/test_web_app.py`, `tests/test_workflows.py`; расширены сценарии склада.

### Изменено

- Конфигурация: переменные `WEB_*`, опциональный запуск без Telegram-токена для веба; обновлены `README.md`, `.env.example`, зависимости.

### Прочее

- В `.gitignore` дополнены шаблоны файлов Cursor (`.cursorignore`, `.cursorrules` и т.д.); лог `legacy/bot_logs.txt` не предназначен для коммита.

## [1.0.0] — 2026-03-28

Первый помеченный релиз.

### Добавлено

- Скрипты локальной среды: `scripts/setup-dev.ps1`, `scripts/setup-dev.sh`.
- Конфигурация `pyproject.toml` для Ruff.
- Настройки рабочей области VS Code: `.vscode/`.
- Константа версии приложения `APP_VERSION` и строка в логе при старте.

### Исправлено

- Экспорт блока «24 часа» в Google Sheets: колонки «Кол-во» и «Стоимость» передаются как числа (без лишних дробных знаков в тексте и без принудительного текстового формата ячеек).

### Прочее

- Файлы Cursor/агентов исключены из репозитория (см. `.gitignore`).
