# API to Iceberg on Oracle Cloud, every 15 minutes

A working pipeline you can hand to Sandbox Factory as-is. It pulls live data
from a public API that needs a key, lands it in Object Storage, turns it into an
Iceberg table with Spark, and gives you three ways to look at the result.

```
API (key required)  ->  OCI Function, every 15 min  ->  Object Storage  raw/
                                                              |
                                                     OCI Data Flow (Spark)
                                                              |
                                            Iceberg table  lake.db.observations
                                            Parquet copy   gold/observations/
                                                              |
                                    query app  |  SQL in the database  |  Select AI
```

Nothing here is Oracle-specific in your code: the function is a plain AWS Lambda
handler and the Spark job is ordinary PySpark. Every setting arrives through the
environment, so the same files run unchanged in another tenancy or region.

## What is in here

| Path | What it is |
|---|---|
| `functions/ingest/lambda_function.py` | The collector. A normal `lambda_handler(event, context)`. Retries, structured logging, writes one JSON-lines object per run. |
| `functions/ingest/requirements.txt` | `requests` and the OCI SDK. |
| `dataflow/bronze_to_iceberg.py` | The Spark job. Reads every raw object, de-duplicates, MERGEs into an Iceberg table, and writes a Parquet copy for SQL. |
| `sql/see_the_data.sql` | Reads the tables from the sandbox's database, including the Select AI examples. |

## The data source

Pick one with `SOURCE`. Each needs its own key, and the key is never written
into the code or the repository.

| `SOURCE` | Data | Key from |
|---|---|---|
| `openweather` (default) | Current conditions per city. Changes every few minutes, nested payload. | https://openweathermap.org/api |
| `finnhub` | Last trade per symbol. Moves only while markets are open. | https://finnhub.io |
| `openaq` | Air-quality sensors per country. | https://openaq.org |

`PLACES` chooses what to pull: cities, ticker symbols, or country codes.

## Deploy it

Paste this into the Sandbox Factory chat, then fill the API key into the
Environment box it offers you:

> Build me a pipeline from https://github.com/ashishsinha1602/oci-iceberg-pipeline
> A function from `functions/ingest` that runs every 15 minutes, a bucket called
> lake, a Data Flow job from `dataflow/bronze_to_iceberg.py` writing Iceberg, and
> a database I can query it from. Turn logging on.

Give it `API_KEY`, and optionally `SOURCE` and `PLACES`. Those values go into the
function and the Spark job as environment variables and are never sent to the
assistant.

## See the data

Three ways, no code changes.

1. **The Iceberg query application.** Every sandbox with Iceberg gets one. Run it
   with a `sql` parameter, for example
   `SELECT place, count(*) FROM lake.db.observations GROUP BY place`.
   Results land in the bucket under `query-results/` as CSV.
2. **SQL in the database.** Run `sql/see_the_data.sql` in SQL Developer Web. It
   creates an external table over the Parquet copy, and shows the Iceberg variant
   commented out underneath.
3. **Plain English.** Select AI is already on: `SELECT AI which place is warmest right now;`

## Where the logs are

| What | Where |
|---|---|
| Every function run, one JSON line each | The sandbox's log group, `Functions invoke`. Linked from the card. |
| Spark driver and executor output | The logs bucket, and the Run page in the console. |
| Each 15-minute firing | OCI Resource Scheduler, one schedule per function. |

## Run it on your laptop first

```bash
cd functions/ingest
pip install -r requirements.txt
export API_KEY=...            # your key
export SOURCE=openweather
export PLACES="London,Tokyo"
export BUCKET_LAKE=...  OBJECT_NAMESPACE=...
python lambda_function.py
```

## Notes worth knowing

- **Oracle's scheduler will not fire more often than hourly.** Sandbox Factory
  splits a 15-minute cron into four schedules, at minute 0, 15, 30 and 45.
- **The Spark job is idempotent.** It MERGEs on source, place and observation
  time, so re-running never doubles a reading.
- **Data Flow has no timer of its own.** Run it by hand, from Airflow, or add a
  second small function that starts the run.
