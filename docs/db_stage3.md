# SQLite Stage 3

## Что добавлено

- Добавлен raw-ingest слой для Excel из Yandex Disk / Telegram / URL в `raw_yadisk_rows`.
- Добавлен сервис `bot/services/yadisk_ingest.py`.
- Raw-ingest подключен в текущие режимы `без движения` и `24ч` параллельно существующей бизнес-логике.
- Добавлен безопасный matching raw-строк к `cases` без изменения самих кейсов.
- Импорты raw-слоя теперь пишутся в `imports`.

## Как теперь работает ingest

- Точка входа: `bot/services/yadisk_ingest.py::ingest_yadisk_rows(...)`.
- Хендлеры вызывают ingest после успешного чтения Excel в режимах:
  - `no_move`
  - `24h`
- Для каждой обработки файла:
  - создаётся запись в `imports` со статусом `running`;
  - Excel читается по листам;
  - для поддерживаемых листов строится каноническая raw-строка;
  - raw-строка пишется в `raw_yadisk_rows`, если это не дубль;
  - выполняется безопасный matching к `cases`;
  - импорт завершается через `finish_import(...)`.

## Что хранится в `raw_yadisk_rows`

Stage 3 использует и/или добавляет поля:

- `import_batch_id`
- `source_file_name`
- `source_path`
- `source_kind`
- `source_sheet_name`
- `source_row_number`
- `row_hash`
- `shk`
- `tare_transfer`
- `item_name`
- `amount`
- `qty_shk`
- `last_movement_at`
- `writeoff_started_at`
- `example_related_shk`
- `normalized_json`
- `matched_case_id`
- `match_method`
- `match_confidence`
- `linked_at`
- `link_decision_reason`
- `imported_at`

Миграция делается мягко:

- через `PRAGMA table_info(...)`;
- только через `ALTER TABLE ADD COLUMN`;
- без пересоздания таблицы.

## Нормализация raw-строк

- Заголовки нормализуются через:
  - lower-case
  - trim
  - сжатие пробелов
  - `ё -> е`
- Значения нормализуются через:
  - trim строк
  - пустые значения -> `None`
  - аккуратный parse чисел
  - аккуратный parse дат в ISO
- В `normalized_json` сохраняется детерминированный snapshot:
  - `source_kind`
  - `source_sheet_name`
  - `source_row_number`
  - `raw_values`
  - `extracted_fields`

## Dedupe

Защита от дублей работает на двух уровнях.

Логика вставки:

- `insert_raw_yadisk_row_if_new(...)` сначала ищет существующую строку.
- Если строка с тем же raw identity уже есть, новая запись не вставляется.

Индекс:

- Добавлен уникальный индекс `idx_raw_yadisk_rows_source_dedupe`.
- Дедуп-ключ:
  - `ifnull(source_path, ifnull(source_file_name, ''))`
  - `ifnull(source_kind, '')`
  - `ifnull(source_sheet_name, '')`
  - `row_hash`

Практический смысл:

- повторная обработка того же файла не плодит дубли;
- одинаковая строка внутри того же файла тоже не дублируется;
- для Yandex Disk в `source_path` используется стабильный идентификатор файла с remote path и `modified`, чтобы новая версия файла не сливалась со старой автоматически;
- для Telegram используется `telegram:<file_unique_id>`;
- для URL используется сам URL.

## Matching

Matching выполняется только к уже существующим кейсам из Google Sheets.

Порядок:

1. `shk`
2. `tare_transfer`
3. `item_name`

Правила:

- Если по текущему правилу найден ровно один кандидат, raw-строка линкуется к `case_id`.
- Если найдено несколько кандидатов, автолинковки нет.
- Если кандидатов нет, происходит переход к следующему правилу.
- Fuzzy matching не используется.

Записываются поля:

- `matched_case_id`
- `match_method`
- `match_confidence`
- `linked_at`
- `link_decision_reason`

Используемые значения confidence:

- `high`
- `medium`
- `low`
- `ambiguous`
- `none`

Важно:

- Yandex Disk / Excel не обновляет `cases`;
- Yandex Disk / Excel не создаёт `case_versions`;
- Yandex Disk / Excel не перетирает кейсы из Google Sheets.

## Интеграция в текущий поток

- Пользовательские команды Telegram-бота не менялись.
- Текущая логика `process_file(...)`, `process_24h_file(...)`, `save_snapshot(...)`, `update_tables(...)` не переписана.
- Raw-ingest запускается как дополнительный шаг.
- Ошибка raw-ingest логируется, но не должна валить основной сценарий обработки Excel.

## Что сознательно не менялось

- Stage 2 sync Google Sheets не переписывался.
- Модель `cases` не менялась радикально.
- Текущая запись в Google Sheets не менялась.
- Существующие JSON snapshot/state для `24ч` и `без движения` не заменялись на SQLite полностью.
- Fuzzy matching не добавлялся.
- Автоматическое создание кейсов из Yandex Disk не добавлялось.

## Что пока не реализовано

- Нет ручного интерфейса разбора ambiguous raw-строк.
- Нет fuzzy / ranked matching.
- Нет полного перевода snapshot-логики `24ч` и `без движения` на чтение из SQLite raw-слоя.
