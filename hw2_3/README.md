# ДЗ 2.3

- **[Отчёт с результатами](REPORT.md)**
- Результаты Parquet — `exports/demo/`; логи и скриншоты — `reports/`.

## Запуск 

Из  каталога проекта:

```powershell
Copy-Item .env.example .env
docker compose up -d --build
```

Веб-интерфейс Airflow: http://localhost:8080. 

Для проверки после трёх `scheduled` запусков:

```powershell
docker compose run --rm --no-deps tools python /opt/airflow/jobs/check_fixtures.py
docker compose run --rm --no-deps tools python /opt/airflow/jobs/validate_results.py --phase before
```

После очистки (Clear) обеих задач **только первого** запуска за 23 января:

```powershell
docker compose run --rm --no-deps tools python /opt/airflow/jobs/validate_results.py --phase after
```

Для выгрузки parquet:

```powershell
docker compose run --rm --no-deps tools python /opt/airflow/scripts/export_results.py --dataset demo
```

