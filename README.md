# Bot_Mont_SHK

Telegram-бот и общая библиотека обработки Excel (пакет `bot-mont-shk`). Веб-сайт [AnniLand](https://github.com/Anniegard/anniland-web) вынесен в отдельный репозиторий и подключает это ядро через `pip`. Оба интерфейса используют одну логику (`bot/services/processing.py` и связанные модули) и пишут результат в Google Spreadsheet.

Основные сценарии:
- «Без движения» (основная выгрузка)
- «24 часа» (прогноз списаний)
- «Задержка склада (сводная)»:
  - из одного сводного `.xlsx` файла;
  - из нескольких Excel/zip из отдельной папки Я.Диска.

Кейс-синк продолжает читать только master-вкладку `Разбор потерь исх. потока`. Экспортные данные пишутся в один worksheet: левый блок для «Без движения», правый блок для «24 часа». Если `WORKSHEET_NAME` не задан или лист не найден, используется `sheet1`.

## Что умеют бот и сайт
- Постоянные кнопки:
  - 📦 Без движения — загрузка основного Excel.
  - ⏱ 24 часа (обновить) — загрузка прогноза, обновление snapshot.
  - 📦 Задержка склада (сводная) — сначала предлагает выбор:
    - «Из одного файла» — принимает единый сводный `.xlsx` с колонкой `Блок`, файл можно прислать документом или взять как последний файл с Я.Диска;
    - «Из нескольких файлов» — читает все Excel/zip из `YANDEX_WAREHOUSE_DELAY_DIR`.
  - ☁️ Взять с Я.Диска (последний файл) — скачать последний файл из папок Я.Диска по OAuth.
  - 📎 Инструкция по загрузке на Диск.
  - 🛠 Админ-панель (только для администраторов из `BOT_ADMIN_IDS` или `ADMIN_USER_ID`).
- Runtime raw DB features сейчас отключены: review/search команды для `raw_yadisk_rows` и кейсов не зарегистрированы в боте.
- Приём входных данных: документ в Telegram до 20 МБ, Яндекс.Диск (OAuth), zip с Excel внутри.
- «Без движения»: группировка по «Гофра», идентификаторы товара собираются через `\n` в одной ячейке, фильтр `Стоимость > 2000`, колонка названа «Идентификатор товара».
- «24 часа»: берётся snapshot, пересекается с картой ID тары из «Без движения», группируется по ID тары, берётся минимальный прогноз по группе, сортируется по времени.
- Логирование действий в `logs/bot.log` (ротация 5MB×3) + stdout.

## Структура Google Sheets
- Master-вкладка для case sync:
  - `Разбор потерь исх. потока` — единственный источник кейсов для синка в БД.
- Экспортный worksheet, левый блок «Без движения»:
  - B2:E2 — заголовок «Выгрузка Идентификатор товара без движения» (объединено)
  - B3:E3 — колонки: `Гофра`, `Идентификатор товара`, `Кол-во`, `Стоимость`
  - Строка 4 пустая (разделитель), данные с B5 без пустых строк между записями.
- Экспортный worksheet, правый блок «24 часа»:
  - K2:O2 — заголовок «Товар, который спишется в течение 24ч» (объединено)
  - K3:O3 — колонки: `ID тары`, `Идентификатор товара`, `Кол-во`, `Стоимость`, `Когда начнёт списываться?`
  - Строка 4 пустая (разделитель), данные с K5 без пустых строк между записями.
  - P2 — «Актуальность файла 24ч:»; P3 — время последнего snapshot.
- Отдельный worksheet для «Задержка склада (сводная)»:
  - Имя листа задаётся `WAREHOUSE_DELAY_WORKSHEET_NAME`, по умолчанию `Выгрузка задержка склада`.
  - При каждом запуске лист полностью очищается и записывается заново.
  - Содержит 3 блока:
    - левый верхний: общий;
    - левый нижний: `без задания`;
    - правый: `Топ 10 тар без задания`.
  - В каждом блоке строки складов идут в фиксированном порядке, внизу есть строка `Общее количество`.
  - В single-режиме склад определяется по колонке `Блок`, значение приводится к каноническому порядку складов.

## Установка и запуск (Ubuntu)
```bash
cd Bot_Mont_SHK
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# или: pip install -e .
pip install -r requirements-dev.txt  # для линтеров/форматтера
cp .env.example .env   # заполните переменные
python main.py
```

Если задан `TELEGRAM_WEBHOOK_URL`, бот автоматически работает в webhook-режиме, иначе остаётся polling. Сайт запускается из репозитория [anniland-web](https://github.com/Anniegard/anniland-web).

### Windows / быстрый сетап

```powershell
.\scripts\setup-dev.ps1
.\.venv\Scripts\Activate.ps1
```

### Версия

Текущий релиз: **1.0.0** (см. `CHANGELOG.md`, `pyproject.toml`, `bot.constants.APP_VERSION`).

Локальные настройки Cursor (`.cursor/`, `AGENTS.md`, шаблоны в `tools/cursor-portable/`) в репозиторий не входят — храните их только у себя.

## Переменные окружения
- `TELEGRAM_TOKEN` или `TELEGRAM_BOT_TOKEN` — токен бота.
- `SPREADSHEET_ID` — ID таблицы.
- `WORKSHEET_NAME` — worksheet для export-блоков; если не задан или лист не найден, бот использует `sheet1`.
- `GOOGLE_CREDENTIALS_PATH` — путь к JSON сервисного аккаунта.
- `ADMIN_USER_ID` — Telegram ID администратора (опционально).
- `BOT_ADMIN_IDS` — список Telegram ID администраторов через запятую (предпочтительный вариант для stage 5).
- `BOT_CONFIG_FILE` — путь к .env, если используется другой файл.
- Telegram webhook (опционально):
  - `TELEGRAM_WEBHOOK_URL` — полный публичный HTTPS URL endpoint'а Telegram, например `https://anniland.ru/tg/webhook/<secret-path>`.
  - `TELEGRAM_WEBHOOK_SECRET` — секрет для заголовка `X-Telegram-Bot-Api-Secret-Token` (рекомендуется обязательным).
  - `TELEGRAM_WEBHOOK_LISTEN` — локальный адрес, где бот слушает webhook (обычно `127.0.0.1`).
  - `TELEGRAM_WEBHOOK_PORT` — локальный порт webhook-сервера бота (например `8081`).
  - `TELEGRAM_WEBHOOK_PATH` — локальный путь webhook (по умолчанию `/tg/webhook`); должен соответствовать пути в `TELEGRAM_WEBHOOK_URL`.
- Яндекс.Диск (OAuth):
  - `YANDEX_OAUTH_TOKEN` — OAuth-токен Диска.
  - `YANDEX_NO_MOVE_DIR` — рекомендуемый путь `disk:/BOT_UPLOADS/no_move/`.
  - `YANDEX_24H_DIR` — рекомендуемый путь `disk:/BOT_UPLOADS/24h/`.
  - `YANDEX_WAREHOUSE_DELAY_DIR` — путь для пакетной сводной задержки склада, по умолчанию `disk:/BOT_UPLOADS/warehouse_delay/`.
  - `YANDEX_ALLOWED_EXTS` — по умолчанию `.xlsx,.xls,.zip`.
  - `YANDEX_MAX_MB` — лимит скачивания по OAuth (по умолчанию 200).
- `WAREHOUSE_DELAY_WORKSHEET_NAME` — имя отдельного worksheet для сводной задержки склада.
- Telegram AI-ассистент (опционально, только для админов):
  - `AI_ENABLED` — включает Telegram-only AI-режим.
  - `AI_PROVIDER` — текущий провайдер (`openai`).
  - `AI_ADMIN_IDS` — отдельный allowlist Telegram ID для AI; если пусто, используется `BOT_ADMIN_IDS`.
  - `OPENAI_API_KEY` / `OPENAI_MODEL` / `OPENAI_BASE_URL` / `OPENAI_TIMEOUT_SECONDS` — настройки OpenAI.
  - `AI_MAX_CONCURRENT_REQUESTS` — лимит одновременных AI-запросов.
  - `AI_MAX_FILES_PER_REQUEST` — максимум источников на один AI-запрос.
  - `AI_MAX_FILE_MB` — лимит размера файла для AI-режима.
  - `AI_MAX_ROWS_PER_SOURCE` / `AI_MAX_SCAN_ROWS_PER_SOURCE` — лимиты детерминированной предобработки таблиц.
  - `AI_MAX_CONTEXT_CHARS` / `AI_MAX_HISTORY_MESSAGES` / `AI_MAX_ANSWER_CHARS` — лимиты контекста, истории и ответа.
  - `AI_MAX_RETRIES` / `AI_RETRY_BACKOFF_MS` / `AI_TEMPERATURE` — настройки устойчивости и генерации.
- Переменные веб-приложения (`WEB_*`, `PUBLIC_BASE_URL`, …) — см. [anniland-web](https://github.com/Anniegard/anniland-web); при общем `.env` с ботом перечень совпадает с прежним.

## Файлы данных
- `data/block_ids.txt` — whitelist ID Блока (по одному в строке). Шаблон: `data/block_ids.txt.example`.
- `data/last_24h_snapshot.json`, `data/last_24h_meta.json` — snapshot 24ч (создаются на VM).
- `data/last_no_move_map.json`, `data/last_no_move_meta.json` — карта идентификатор → ID тары из «Без движения» (создаются на VM).
- `data/README.md` — описание содержимого папки `data/`.

## Использование
1. /start — появляется клавиатура.
2. Выберите режим:
   - 📦 Без движения — отправьте Excel (до 20 МБ) или нажмите «☁️ Взять с Я.Диска…».
   - ⏱ 24 часа — отправьте прогноз или нажмите «☁️ Взять с Я.Диска…»; правый блок обновится сразу, если есть карта ID тары.
   - 📦 Задержка склада (сводная):
     - «Из одного файла» — отправьте единый сводный `.xlsx` или нажмите «☁️ Взять с Я.Диска…».
     - «Из нескольких файлов» — бот сам скачает все файлы из `YANDEX_WAREHOUSE_DELAY_DIR` и обновит отдельный worksheet.
3. «📎 Инструкция…» — напоминает, куда класть файлы на Я.Диск.
4. Админ-панель — только для администраторов из `BOT_ADMIN_IDS` или `ADMIN_USER_ID`.
5. Telegram AI-режим — только для админов:
   - `/ai` — включить AI-режим;
   - `/ai_use no_move|24h|warehouse_delay` — добавить проектный источник;
   - можно прислать `.xlsx/.xls/.csv/.zip` как AI-источник;
   - `/ai_reset` — очистить AI-контекст;
   - `/ai_exit` — выйти из AI-режима.
6. Runtime raw DB review/search команды сейчас отключены и в Telegram не зарегистрированы.

## Яндекс.Диск (OAuth, без публичных ссылок)
- Создайте папки `disk:/BOT_UPLOADS/no_move/`, `disk:/BOT_UPLOADS/24h/` и `disk:/BOT_UPLOADS/warehouse_delay/`.
- Токен должен иметь доступ к этим папкам.
- При нажатии «☁️ Взять с Я.Диска…» бот берёт самый свежий Excel/zip из соответствующей папки.
- Для `📦 Задержка склада (сводная) → Из одного файла` кнопка «☁️ Взять с Я.Диска…» берёт последний файл из `YANDEX_WAREHOUSE_DELAY_DIR`.
- Для `📦 Задержка склада (сводная) → Из нескольких файлов` бот читает все подходящие файлы из `YANDEX_WAREHOUSE_DELAY_DIR`, скачивает их по очереди во временную директорию и после обработки удаляет временные файлы.

## Деплой бота на VM
- Шаблон systemd: `deploy/bot-mont-shk-bot.service` (и дубликат в `deploy/systemd/`).
- Рекомендуемая схема:
  - `nginx -> 127.0.0.1:8081` для Telegram webhook (`/tg/webhook/...`);
  - отдельный systemd-сервис для `python main.py`;
  - сайт и nginx для `anniland.ru` — в репозитории [anniland-web](https://github.com/Anniegard/anniland-web) (`deploy/` там);
  - `.env` можно хранить один на оба процесса или разделить; пути к `data/` настройте так, чтобы бот и веб видели одни и те же файлы snapshot/карт (часто symlink общего каталога `data/`).
- Перед включением HTTPS настройте DNS `anniland.ru` на VM и выпустите сертификат Let's Encrypt.

### Чеклист переключения Telegram на webhook
1. На VM задать в `.env` переменные `TELEGRAM_WEBHOOK_*` и убедиться, что `TELEGRAM_WEBHOOK_URL` доступен снаружи по HTTPS.
2. В nginx добавить `location` для `/tg/webhook/` с проксированием на `127.0.0.1:8081`.
3. Перезапустить nginx и сервис бота.
4. Сбросить старый webhook:
   - `curl -s "https://api.telegram.org/bot$TELEGRAM_TOKEN/deleteWebhook?drop_pending_updates=true"`
5. Установить новый webhook:
   - `curl -s -X POST "https://api.telegram.org/bot$TELEGRAM_TOKEN/setWebhook" -d "url=$TELEGRAM_WEBHOOK_URL" -d "secret_token=$TELEGRAM_WEBHOOK_SECRET"`
6. Проверить статус:
   - `curl -s "https://api.telegram.org/bot$TELEGRAM_TOKEN/getWebhookInfo"`
   - убедиться, что `ok=true`, `pending_update_count` не растёт бесконечно, `last_error_message` пустой.
7. Smoke-тест: отправить боту `/start` и проверить ответ + отсутствие polling-процесса.

## Линтеры и форматирование
- Dev-зависимости: `black`, `ruff`, `pytest` в `requirements-dev.txt`.
- CI (GitHub Actions): `python -m compileall .`, `ruff check . --select F,E9` и `pytest`.

## Тесты
- Локальный запуск:
```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
pytest
```
- Покрыто минимальными regression-тестами:
  - dedupe для `case_versions`, `raw_sheet_rows`, `raw_yadisk_rows`;
  - приоритетный matching `shk -> tare_transfer -> item_name`;
  - ambiguous matching без авто-линковки;
  - сохранение существующего `case_id` без перегенерации;
  - manual link / manual unlink / ignore / mark pending для raw review;
  - case search по `case_id`;
  - приоритет поиска кейса по `ШК` над `тарой/передачей`;
  - linked raw rows для кейса;
  - raw search по `ШК` и `таре/передаче`;
  - parsing `BOT_ADMIN_IDS` / `ADMIN_USER_ID`.
  - warehouse delay: нормализация имени файла, mapping в canonical row name, bucketization, фильтр `без задания`, total row, пропуск непонятного файла, формирование Google Sheets matrix.
  - warehouse delay single-file: группировка по колонке `Блок`, top-10 тар без задания, dedupe по таре, пропуск невалидного времени.
  - Telegram AI: конфиг OpenAI/лимитов, context builder, source loader, handler routing и mock-based provider calls.
- Сознательно не покрыто на этом этапе:
  - живые интеграции с Google Sheets и Yandex Disk;
  - end-to-end обработка реальных Excel-файлов;
  - Telegram handlers и сценарии с `.env`/секретами.

## Примечания
- Пустые/невалидные прогнозы 24ч исключаются; группы без корректного прогноза не выводятся.
- Фильтр 24ч по `ID Блока` использует `data/block_ids.txt`; пустой файл = без фильтра.
- Для warehouse delay список алиасов колонок и нормализация имён файлов находятся в `bot/services/warehouse_delay.py`.
- Single-файл warehouse delay должен быть единым сводным `.xlsx`; для определения склада используется колонка `Блок`.
- Одна обработка за раз (async lock).
- Документы в Telegram — до 20 МБ; большие файлы — только через Я.Диск (кнопка «Взять с Я.Диска»).

Точка входа бота: `python main.py`. Веб-приложение — репозиторий [anniland-web](https://github.com/Anniegard/anniland-web). Папка `legacy/` содержит старую версию бота для справки.
