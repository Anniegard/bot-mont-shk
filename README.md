# Bot_Mont_SHK

Telegram-бот принимает два типа файлов и пишет результат на один лист Google Sheets:
- «Без движения» (основная выгрузка)
- «24 часа» (прогноз списаний)

Данные выводятся в две зоны листа. Между каждой строкой данных вставляется одна пустая строка, чтобы можно было выделить блок `CTRL+A` и копировать.

## Что делает бот
- Постоянные кнопки:
  - 📦 Без движения — загрузка основного Excel.
  - ⏱ 24 часа (обновить) — загрузка прогноза, обновление snapshot.
  - ☁️ Взять с Я.Диска (последний файл) — скачать последний файл из папок Я.Диска по OAuth.
  - 📎 Инструкция по загрузке на Диск.
  - 🛠 Админ-панель (только для администраторов из `BOT_ADMIN_IDS` или `ADMIN_USER_ID`).
- Admin review layer для `raw_yadisk_rows`: просмотр очереди unresolved строк, детали, кандидаты, ручная привязка к существующему `case_id`, явный unlink, ignore/re-open, audit trail.
- Приём входных данных: документ в Telegram до 20 МБ, прямая ссылка, Яндекс.Диск (OAuth, без публичных ссылок), zip с Excel внутри.
- «Без движения»: группировка по «Гофра», идентификаторы товара собираются через `\n` в одной ячейке, фильтр `Стоимость > 2000`, колонка названа «Идентификатор товара».
- «24 часа»: берётся snapshot, пересекается с картой ID тары из «Без движения», группируется по ID тары, берётся минимальный прогноз по группе, сортируется по времени.
- Логирование действий в `logs/bot.log` (ротация 5MB×3) + stdout.

## Структура листа Google Sheets
- Левая зона («Без движения»):
  - B2:E2 — заголовок «Выгрузка Идентификатор товара без движения» (объединено)
  - B3:E3 — колонки: `Гофра`, `Идентификатор товара`, `Кол-во`, `Стоимость`
  - Строка 4 пустая (разделитель), данные с B5 без пустых строк между записями.
- Правая зона («24 часа»):
  - K2:O2 — заголовок «Товар, который спишется в течение 24ч» (объединено)
  - K3:O3 — колонки: `ID тары`, `Идентификатор товара`, `Кол-во`, `Стоимость`, `Когда начнёт списываться?`
  - Строка 4 пустая (разделитель), данные с K5 без пустых строк между записями.
- P3 — «Актуальность файла 24ч:»; P4 — время последнего snapshot.

## Установка и запуск (Ubuntu)
```bash
cd Bot_Mont_SHK
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt  # для линтеров/форматтера
cp .env.example .env   # заполните переменные
python main.py
```

## Переменные окружения
- `TELEGRAM_TOKEN` или `TELEGRAM_BOT_TOKEN` — токен бота.
- `SPREADSHEET_ID` — ID таблицы.
- `WORKSHEET_NAME` — имя листа (опционально, по умолчанию первый).
- `GOOGLE_CREDENTIALS_PATH` — путь к JSON сервисного аккаунта.
- `ADMIN_USER_ID` — Telegram ID администратора (опционально).
- `BOT_ADMIN_IDS` — список Telegram ID администраторов через запятую (предпочтительный вариант для stage 5).
- `BOT_CONFIG_FILE` — путь к .env, если используется другой файл.
- Яндекс.Диск (OAuth):
  - `YANDEX_OAUTH_TOKEN` — OAuth-токен Диска.
  - `YANDEX_NO_MOVE_DIR` — рекомендуемый путь `disk:/BOT_UPLOADS/no_move/`.
  - `YANDEX_24H_DIR` — рекомендуемый путь `disk:/BOT_UPLOADS/24h/`.
  - `YANDEX_ALLOWED_EXTS` — по умолчанию `.xlsx,.xls,.zip`.
  - `YANDEX_MAX_MB` — лимит скачивания по OAuth (по умолчанию 200).

## Файлы данных
- `data/block_ids.txt` — whitelist ID Блока (по одному в строке). Шаблон: `data/block_ids.txt.example`.
- `data/last_24h_snapshot.json`, `data/last_24h_meta.json` — snapshot 24ч (создаются на VM).
- `data/last_no_move_map.json`, `data/last_no_move_meta.json` — карта идентификатор → ID тары из «Без движения» (создаются на VM).
- `data/README.md` — описание содержимого папки `data/`.

## Использование
1. /start — появляется клавиатура.
2. Выберите режим:
   - 📦 Без движения — отправьте Excel (до 20 МБ) или ссылку, либо нажмите «☁️ Взять с Я.Диска…».
   - ⏱ 24 часа — отправьте прогноз или нажмите «☁️ Взять с Я.Диска…»; правый блок обновится сразу, если есть карта ID тары.
3. «📎 Инструкция…» — напоминает, куда класть файлы на Я.Диск.
4. Админ-панель — только для администраторов из `BOT_ADMIN_IDS` или `ADMIN_USER_ID`.
5. Admin review-команды для unresolved raw-строк:
   - `/raw_help`
   - `/raw_queue [limit] [source_kind]`
   - `/raw_show <raw_id>`
   - `/raw_candidates <raw_id>`
   - `/raw_link <raw_id> <case_id> [note]`
   - `/raw_unlink <raw_id> [note]`
   - `/raw_ignore <raw_id> [note]`
   - `/raw_pending <raw_id> [note]`
   - `/raw_pending` только возвращает строку в review queue.
   - `/raw_unlink` явно снимает связь с кейсом и тоже возвращает строку в `pending`.
   - Google Sheets остаётся master-источником данных кейса; review-команды не меняют `cases` и не создают новые кейсы.

## Яндекс.Диск (OAuth, без публичных ссылок)
- Создайте папки `disk:/BOT_UPLOADS/no_move/` и `disk:/BOT_UPLOADS/24h/`.
- Токен должен иметь доступ к этим папкам.
- При нажатии «☁️ Взять с Я.Диска…» бот берёт самый свежий Excel/zip из соответствующей папки.

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
  - parsing `BOT_ADMIN_IDS` / `ADMIN_USER_ID`.
- Сознательно не покрыто на этом этапе:
  - живые интеграции с Google Sheets и Yandex Disk;
  - end-to-end обработка реальных Excel-файлов;
  - Telegram handlers и сценарии с `.env`/секретами.

## Примечания
- Пустые/невалидные прогнозы 24ч исключаются; группы без корректного прогноза не выводятся.
- Фильтр 24ч по `ID Блока` использует `data/block_ids.txt`; пустой файл = без фильтра.
- Одна обработка за раз (async lock).
- Документы в Telegram — до 20 МБ; большие файлы — ссылка или Я.Диск.

Актуальная точка входа: python main.py. Папка legacy/ содержит старую версию бота для справки.
