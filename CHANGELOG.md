# Changelog

## [1.4.1] — 2026-04-23

### Исправлено

- Режимы выгрузки «с передачами/без передач» в `bot/services/excel.py`: префикс `5...` теперь корректно считается передачей (наравне с `3...` и `4...`) и не попадает в список нестандартных значений.

## [1.4.0] — 2026-04-12

### Исправлено

- Яндекс.Диск (`bot/services/yadisk.py`): конечные таймауты aiohttp при скачивании и запросах API; понятные `YaDiskError` при таймаутах и сетевых сбоях (без утечки токена в сообщениях).
- Обработка (`bot/services/processing.py`): опциональный `progress_cb` для web, пошаговые логи; `WorkflowError` для повреждённого ZIP / отсутствия Excel в архиве и сбоев Google Sheets / снимка; детальные этапы для режима «24 часа».

### Изменено

- Веб-интерфейс AnniLand вынесен в отдельный репозиторий [anniland-web](https://github.com/Anniegard/anniland-web). Пакет `bot-mont-shk` устанавливается из этого репозитория (`pip install` / `pip install git+https://...`) и остаётся единственным местом бизнес-логики обработки.
- Зависимости FastAPI/uvicorn/Jinja удалены из `requirements.txt` бота — они нужны только веб-проекту.

## [1.3.0] — 2026-04-12

### Добавлено

- Публичная главная `/` — одностраничное портфолио (контент в `web/content/portfolio_ru.py`, вёрстка `web/templates/portfolio.html`, стили `web/static/public.css`), SEO (meta, Open Graph, JSON-LD Person), favicon `web/static/favicon.png`.
- Алиас маршрута `/workspace` → редирект на `/app`.
- Опционально: `PUBLIC_PHONE`, `PUBLIC_OG_IMAGE_URL` в конфиге для телефона на лендинге и картинки OG.

### Изменено

- Шаблоны рабочей зоны переведены на `base_app.html`; публичная часть использует `base_public.html`.
- Удалены неиспользуемые `landing.html` и `index.html`.

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
