# Итоговый проект DWH

**Результаты:** [отчёт о выполнении](reports/REPORT.md), [условие](REQUIREMENTS.md).

## Запуск


```powershell
Copy-Item .env.example .env
docker compose up -d --build
docker compose ps -a
docker compose run --rm tools python /opt/airflow/scripts/bootstrap.py
docker compose run --rm tools python /opt/airflow/scripts/replay_kafka.py --dataset /opt/airflow/data/fixture --bootstrap-servers kafka:19092 --until-arrival 2026-01-04T00:00:00Z --rate 0
docker compose run --rm tools python /opt/airflow/scripts/archive_status.py
```

После получения 26 сообщений событий и 8 изменений профиля включить DAG `cinema_daily_dwh` в [Airflow](http://localhost:8082).

Пример запроса в [ClickHouse](http://localhost:8125/play):

```sql
SELECT sum(views), sum(watched_seconds) FROM cinema.final_minute;
```

После полного испытания `fixture` ожидаются 20 просмотров, 3141 с. Остальные запросы: [sql/report_examples.sql](sql/report_examples.sql).
