"""Raw JSON in Object Storage -> an Iceberg table, plus a Parquet copy to read from SQL.

Runs on OCI Data Flow (managed Spark, the counterpart of an AWS Glue job).
Sandbox Factory configures the Iceberg catalog for this job: a catalog named
`lake`, warehouse oci://<bucket>@<namespace>/iceberg. Nothing here hard-codes a
tenancy: every setting arrives as spark.sandbox.<NAME>.

It writes two things on purpose:
  lake.db.observations      the Iceberg table, with history and snapshots
  gold/observations/        the current rows as Parquet, so an Autonomous
                            Database can read them as an external table
"""
import datetime as dt
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

TABLE = "lake.db.observations"


def setting(spark, name, default=None, required=False):
    """A value the sandbox injected, e.g. BUCKET_LAKE -> spark.sandbox.BUCKET_LAKE."""
    value = spark.conf.get(f"spark.sandbox.{name}", default)
    if required and not value:
        raise SystemExit(f"spark.sandbox.{name} is missing; the sandbox sets it when you ask for a bucket")
    return value


def main():
    spark = (SparkSession.builder.appName("bronze_to_iceberg").getOrCreate())
    spark.sparkContext.setLogLevel("INFO")

    bucket = setting(spark, "BUCKET_LAKE") or setting(spark, "DATA_BUCKET", required=True)
    namespace = setting(spark, "OBJECT_NAMESPACE", required=True)
    base = f"oci://{bucket}@{namespace}"
    raw_path = f"{base}/raw/"
    gold_path = f"{base}/gold/observations"

    print(f"[pipeline] reading {raw_path}", flush=True)
    raw = spark.read.option("recursiveFileLookup", "true").json(raw_path)
    if raw.rdd.isEmpty():
        print("[pipeline] nothing to do: no raw objects yet. The function writes one every 15 minutes.", flush=True)
        spark.stop()
        return

    numeric = ["lat", "lon", "temperature_c", "feels_like_c", "humidity_pct",
               "pressure_hpa", "wind_speed_ms", "wind_deg", "cloud_pct", "visibility_m"]
    for c in numeric:
        if c not in raw.columns:
            raw = raw.withColumn(c, F.lit(None).cast(DoubleType()))
    for c in ["place", "country", "condition", "description", "source", "run_id", "raw"]:
        if c not in raw.columns:
            raw = raw.withColumn(c, F.lit(None).cast("string"))

    df = (raw
          .withColumn("observed_at", F.to_timestamp("observed_at"))
          .withColumn("ingested_at", F.to_timestamp("ingested_at"))
          .withColumn("observed_date", F.to_date("observed_at"))
          .select("source", "place", "country", "observed_at", "observed_date", "ingested_at",
                  "condition", "description",
                  *[F.col(c).cast(DoubleType()).alias(c) for c in numeric],
                  "run_id", "raw")
          .dropDuplicates(["source", "place", "observed_at"]))

    rows = df.count()
    print(f"[pipeline] {rows} distinct readings, {df.select('place').distinct().count()} places", flush=True)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.db")
    exists = spark.catalog.tableExists(TABLE)
    if not exists:
        print(f"[pipeline] creating {TABLE}", flush=True)
        (df.writeTo(TABLE)
           .partitionedBy(F.col("observed_date"))
           .tableProperty("format-version", "2")
           .createOrReplace())
    else:
        # MERGE keeps the table idempotent: re-running never doubles a reading.
        df.createOrReplaceTempView("incoming")
        spark.sql(f"""
            MERGE INTO {TABLE} t
            USING incoming s
              ON t.source = s.source AND t.place = s.place AND t.observed_at = s.observed_at
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"[pipeline] merged into {TABLE}", flush=True)

    total = spark.table(TABLE).count()
    print(f"[pipeline] {TABLE} now holds {total} rows", flush=True)
    spark.sql(f"SELECT place, count(*) readings, round(avg(temperature_c), 2) avg_value "
              f"FROM {TABLE} GROUP BY place ORDER BY place").show(50, False)

    # a one-file CSV anyone can open straight from the bucket, no Spark needed
    try:
        summary = spark.sql(f"""
            SELECT place,
                   count(*)                          AS readings,
                   round(avg(temperature_c), 2)      AS avg_value,
                   round(min(temperature_c), 2)      AS min_value,
                   round(max(temperature_c), 2)      AS max_value,
                   max(observed_at)                  AS last_seen
              FROM {TABLE} GROUP BY place ORDER BY place""")
        (summary.coalesce(1).write.mode("overwrite")
                .option("header", "true").csv(f"{base}/query-results/latest-summary"))
        print(f"[pipeline] summary CSV written to {base}/query-results/latest-summary", flush=True)
    except Exception as e:  # noqa: BLE001  a convenience, never a reason to fail the job
        print(f"[pipeline] summary CSV skipped: {type(e).__name__}: {e}", flush=True)

    # a plain Parquet copy, so SQL in the database can read it with no extra setup
    (spark.table(TABLE)
          .drop("raw")
          .write.mode("overwrite").partitionBy("observed_date").parquet(gold_path))
    print(f"[pipeline] Parquet copy written to {gold_path}", flush=True)
    print(f"[pipeline] done at {dt.datetime.utcnow().isoformat()}Z", flush=True)
    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
