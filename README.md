# ДЗ 5 — Spark: RDD, ліниві обчислення і паралелізм

Звіт: [REPORT.md](REPORT.md).

## Склад

| Файл | Що це |
|---|---|
| `docker-compose.yml`, `conf/` | Spark 4.2.0 (`apache/spark:4.2.0-python3`), `local[4]`, heap 2 ГБ, event log + History Server на :18080 |
| `generate_transactions.py` | генератор даних із ДЗ 4, обсяг збільшено до 6 млн рядків, seed 42 |
| `my_part1.py` | частина 1: ДЗ 4 одним ланцюжком RDD |
| `my_part2.py` | частина 2: режими `lazy`, `bug`, `nocache`, `cache` |
| `my_part3.py` | частина 3: та сама дія, параметри лише через `spark-submit` |
| `q2_trace.py` | допоміжний: шлях рядків 100–102 через `parse()` до результату Завдання 1 (питання 2) |
| `ui_report.py` | допоміжний: вивантажує jobs/stages/storage/executors із REST API History Server |
| `run_all.sh` | запускає частини 1–3 послідовно і зберігає вивід у `evidence/` |
| `evidence/` | вивід скриптів, дані REST API, скриншоти (`evidence/screens/`), бонус MapReduce |

## Запуск

```bash
python3 generate_transactions.py                 # -> data/transactions.csv (249 645 507 B)
chmod 777 spark-events                           # event log пише користувач spark (uid 185) з образу
docker compose up -d history                     # http://localhost:18080

docker compose run --rm --service-ports spark spark-submit my_part1.py
docker compose run --rm --service-ports spark spark-submit my_part2.py lazy|bug|nocache|cache
docker compose run --rm --service-ports spark spark-submit --master "local[1]" my_part3.py
docker compose run --rm --service-ports spark spark-submit --master "local[4]" \
    --conf spark.hadoop.fs.local.block.size=134217728 my_part3.py
```

`HOLD_UI=60` (через `docker compose run -e HOLD_UI=60 ...`) у режимі `cache` тримає живий UI на :4040
відкритим після замірів, щоб зняти вкладку Storage: History Server її для завершених застосунків не показує.
