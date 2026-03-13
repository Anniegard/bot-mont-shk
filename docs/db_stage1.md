# SQLite Stage 1

## Где находится БД

- Переменная окружения: `BOT_DB_PATH`
- Значение по умолчанию: `data/bot.db`
- Директория для файла БД создаётся автоматически при старте.

## Как инициализируется БД

- Точка входа: `main.py`
- После загрузки `.env` и настройки логирования вызывается `bot.db.init_db(config.db_path)`.
- При каждом подключении включаются:
  - `PRAGMA foreign_keys = ON`
  - попытка `PRAGMA journal_mode = WAL`
  - `sqlite3.Row` через `row_factory`

## Какие таблицы создаются

- `cases` — бизнес-карточка кейса
- `case_versions` — история версий строки кейса из Google Sheets
- `case_items` — дочерние товарные элементы кейса
- `raw_sheet_rows` — сырые строки из Google Sheets
- `raw_yadisk_rows` — сырые строки из Yandex Disk / Excel
- `imports` — журнал импортов
- `sheet_sync_state` — служебное состояние синхронизации листа

Индексы создаются идемпотентно для полей:
- `case_id`
- `row_hash`
- `sheet_name`
- `shk`
- `tare_transfer`
- `item_name`

## Что сделано на первом этапе

- Добавлен модуль `bot/db.py` на стандартном `sqlite3`.
- Реализованы:
  - `init_db()`
  - `get_db_connection()`
  - `insert_import(...)`
  - `finish_import(...)`
  - `upsert_case(...)`
  - `insert_case_version(...)`
  - `insert_case_item(...)`
  - `insert_raw_sheet_row(...)`
  - `insert_raw_yadisk_row(...)`
  - `get_case_by_case_id(...)`
  - `find_case_candidates(...)`
- Добавлены утилиты:
  - нормализация пустых значений
  - ISO UTC timestamp
  - сериализация JSON
  - `row_hash`
- Добавлены константы для будущего master-листа и техколонки `case_id`.
- В `bot/services/sheets.py` добавлены безопасные helper-функции чтения строк листа с распознаванием `case_id`, если колонка уже существует.

## Что сознательно НЕ сделано на первом этапе

- Не менялся текущий пользовательский поток Telegram-бота.
- Не менялась текущая запись в Google Sheets.
- Не переписывалась интеграция с Yandex Disk.
- Не заменялись текущие JSON snapshot/state на SQLite.
- Не выполнялась автоматическая модификация live-таблицы Google Sheets.
- Не включалось автоматическое связывание Yandex Disk -> кейс.
