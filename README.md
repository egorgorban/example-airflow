# example-airflow-dags

Пет-проект с примерами ETL-даг для Apache Airflow. Демонстрирует типовые паттерны загрузки данных из внешних источников в DWH: постраничная выгрузка из HTTP API, работа с буферными таблицами, идемпотентная перезапись по дате, загрузка из Google Sheets.

Даги основаны на реальных коммерческих пайплайнах, но очищены от конфиденциальной информации (реальных ID счётчиков, таблиц, названий компаний и ключей) и адаптированы под демонстрационный стенд.

## Стек

- Apache Airflow 2.10.5, Python 3.12
- CeleryExecutor, PostgreSQL (метабаза Airflow и DWH), Redis (брокер)
- Docker / docker-compose для локального запуска
- pandas, httpx, psycopg2 — обработка и загрузка данных

## Что внутри

| DAG | Источник | Описание |
| --- | --- | --- |
| `ymetrika.hits` | Yandex Metrika Logs API | Постраничная выгрузка hits по нескольким счётчикам через logs-api, буферная таблица → ODL |
| `gsheets.*` | Google Sheets | Полная перезагрузка листов гугл-таблиц в ODL раз в день |
| `test.connections` | PostgreSQL | Служебный dag для проверки подключения к DWH |

Структура каждого источника в `dags/<source>/`:

```
dags/<source>/
├── config.py       # список сущностей/аккаунтов, id таблиц, connection id
├── dag.py          # определение DAG
├── tasks/          # python-callables с бизнес-логикой загрузки
├── schemas/        # описание полей и типов колонок
└── sql/            # DDL и SQL для merge буфер → ODL
```

## Запуск локально

```bash
echo "AIRFLOW_UID=$(id -u)" > .env   # см. .gitignore, файл .env не коммитится
docker compose up -d
```

Веб-интерфейс будет доступен на `http://localhost:8080` (логин/пароль `airflow`/`airflow` — только для локального стенда).

Перед реальным использованием:
- задать `secret_key` / `internal_api_secret_key` в `airflow.cfg` (в репозитории они оставлены пустыми);
- создать Airflow-подключения (`Admin → Connections`) для `dwh-postgres`, `ymetrika-logs-api`, `gcp-example` со своими реальными значениями;
- подставить свои `counter_id` / `spreadsheet_id` в `config.py` соответствующих даг.

## Лицензия

[MIT](LICENSE)
