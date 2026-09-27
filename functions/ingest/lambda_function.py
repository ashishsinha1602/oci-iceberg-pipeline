"""Pull live readings from a public API and land them in OCI Object Storage.

Runs as an OCI Function on a 15-minute schedule. The handler is an ordinary AWS
Lambda handler - Sandbox Factory wraps it, so the same file runs on Lambda and
on OCI Functions with no edit.

What it needs (all from the environment, nothing hard-coded):
  API_KEY           the provider's key. Given on the Environment box when the
                    sandbox is created; it never reaches the assistant.
  SOURCE            openweather (default) | finnhub | openaq
  PLACES            comma-separated: cities for openweather, symbols for
                    finnhub, country codes for openaq
  TRIGGER_DATAFLOW  1 (default) starts the Spark job as soon as the object is
                    written, so the Iceberg table refreshes every run. Set to 0
                    to leave the job to a schedule or to a hand-run.
  DATAFLOW_APP      which Data Flow application to start. Default: the one whose
                    name ends in -bronze-to-iceberg.
  BUCKET_LAKE       set by the sandbox: the bucket to write to
  OBJECT_NAMESPACE  set by the sandbox
Each run writes one JSON-lines object under raw/dt=<date>/hh=<hour>/, so the
Spark job can read the whole history with a single path.
"""
import datetime as dt
import io
import json
import os
import sys
import time
import uuid

import oci
import requests

SOURCE = os.environ.get("SOURCE", "openweather").strip().lower()
API_KEY = os.environ.get("API_KEY", "").strip()
TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "20"))
RETRIES = int(os.environ.get("HTTP_RETRIES", "3"))
DEFAULT_PLACES = {
    "openweather": "London,Tokyo,New York,Mumbai,Sao Paulo,Nairobi,Sydney,Reykjavik",
    "finnhub": "AAPL,MSFT,NVDA,ORCL,TSLA,AMZN",
    "openaq": "GB,JP,US,IN,BR",
}


def log(event, **fields):
    """One JSON object per line: the function log group keeps these."""
    print(json.dumps({"ts": dt.datetime.now(dt.timezone.utc).isoformat(), "event": event, **fields}), flush=True)


def get(url, params=None, headers=None):
    """GET with retries. Raises after the last attempt."""
    last = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
            if r.status_code == 429 or 500 <= r.status_code < 600:
                raise RuntimeError(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last = e
            log("http_retry", url=url.split("?")[0], attempt=attempt, error=str(e)[:200])
            if attempt < RETRIES:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"gave up after {RETRIES} attempts: {last}")


def fetch_openweather(places):
    """Current conditions per city. Nested payload, changes every few minutes."""
    out = []
    for city in places:
        d = get("https://api.openweathermap.org/data/2.5/weather",
                params={"q": city, "appid": API_KEY, "units": "metric"})
        main, wind, sys_ = d.get("main") or {}, d.get("wind") or {}, d.get("sys") or {}
        weather = (d.get("weather") or [{}])[0]
        out.append({
            "place": city,
            "observed_at": dt.datetime.fromtimestamp(d["dt"], dt.timezone.utc).isoformat(),
            "country": sys_.get("country"),
            "lat": (d.get("coord") or {}).get("lat"),
            "lon": (d.get("coord") or {}).get("lon"),
            "condition": weather.get("main"),
            "description": weather.get("description"),
            "temperature_c": main.get("temp"),
            "feels_like_c": main.get("feels_like"),
            "humidity_pct": main.get("humidity"),
            "pressure_hpa": main.get("pressure"),
            "wind_speed_ms": wind.get("speed"),
            "wind_deg": wind.get("deg"),
            "cloud_pct": (d.get("clouds") or {}).get("all"),
            "visibility_m": d.get("visibility"),
            "raw": json.dumps(d, separators=(",", ":")),
        })
    return out


def fetch_finnhub(places):
    """Last trade per symbol."""
    out = []
    for sym in places:
        d = get("https://finnhub.io/api/v1/quote", params={"symbol": sym, "token": API_KEY})
        if not d.get("t"):
            log("skipped", symbol=sym, reason="no timestamp in response")
            continue
        out.append({
            "place": sym,
            "observed_at": dt.datetime.fromtimestamp(d["t"], dt.timezone.utc).isoformat(),
            "country": None, "lat": None, "lon": None,
            "condition": "quote", "description": None,
            "temperature_c": d.get("c"),          # current price
            "feels_like_c": d.get("pc"),          # previous close
            "humidity_pct": d.get("dp"),          # percent change
            "pressure_hpa": d.get("d"),           # absolute change
            "wind_speed_ms": d.get("h"),          # day high
            "wind_deg": d.get("l"),               # day low
            "cloud_pct": None, "visibility_m": None,
            "raw": json.dumps(d, separators=(",", ":")),
        })
    return out


def fetch_openaq(places):
    """Latest air-quality measurements per country."""
    out = []
    for country in places:
        d = get("https://api.openaq.org/v3/locations",
                params={"iso": country, "limit": 25},
                headers={"X-API-Key": API_KEY})
        for loc in d.get("results", []):
            for s in loc.get("sensors", []) or []:
                latest = s.get("latest") or {}
                if latest.get("value") is None:
                    continue
                out.append({
                    "place": f"{country}/{loc.get('name')}/{(s.get('parameter') or {}).get('name')}",
                    "observed_at": (latest.get("datetime") or {}).get("utc"),
                    "country": country,
                    "lat": (loc.get("coordinates") or {}).get("latitude"),
                    "lon": (loc.get("coordinates") or {}).get("longitude"),
                    "condition": (s.get("parameter") or {}).get("name"),
                    "description": (s.get("parameter") or {}).get("units"),
                    "temperature_c": latest.get("value"),
                    "feels_like_c": None, "humidity_pct": None, "pressure_hpa": None,
                    "wind_speed_ms": None, "wind_deg": None, "cloud_pct": None, "visibility_m": None,
                    "raw": json.dumps(s, separators=(",", ":")),
                })
    return out


FETCHERS = {"openweather": fetch_openweather, "finnhub": fetch_finnhub, "openaq": fetch_openaq}


def object_storage():
    """Resource principal inside OCI Functions; API key when run on a laptop."""
    try:
        signer = oci.auth.signers.get_resource_principals_signer()
        return oci.object_storage.ObjectStorageClient({}, signer=signer)
    except Exception:  # noqa: BLE001
        cfg = oci.config.from_file()
        return oci.object_storage.ObjectStorageClient(cfg)


def start_dataflow(run_id):
    """Start the Spark job that turns the raw objects into the Iceberg table.

    Nothing here names a tenancy: the compartment comes from the sandbox and the
    application is found by name. A failure is logged, never raised - the data is
    already safely written and the next run will pick it up.
    """
    compartment = os.environ.get("SANDBOX_COMPARTMENT_OCID")
    if not compartment:
        log("dataflow_skipped", reason="SANDBOX_COMPARTMENT_OCID not set")
        return None
    suffix = os.environ.get("DATAFLOW_APP", "-bronze-to-iceberg")
    try:
        signer = oci.auth.signers.get_resource_principals_signer()
        client = oci.data_flow.DataFlowClient({}, signer=signer)
    except Exception:  # noqa: BLE001  running on a laptop
        client = oci.data_flow.DataFlowClient(oci.config.from_file())
    apps = [a for a in client.list_applications(compartment_id=compartment).data
            if a.display_name.endswith(suffix)]
    if not apps:
        log("dataflow_skipped", reason=f"no application ending in {suffix}")
        return None
    app = apps[0]
    running = [r for r in client.list_runs(compartment_id=compartment, application_id=app.id).data
               if r.lifecycle_state in ("ACCEPTED", "IN_PROGRESS")]
    if running:
        log("dataflow_skipped", reason="a run is already in progress", run=running[0].id[-12:])
        return None
    started = client.create_run(oci.data_flow.models.CreateRunDetails(
        application_id=app.id, compartment_id=compartment,
        display_name=f"bronze-to-iceberg {dt.datetime.now(dt.timezone.utc):%Y-%m-%d %H:%M}")).data
    log("dataflow_started", application=app.display_name, run=started.id[-12:], after_run=run_id)
    return started.id


def lambda_handler(event, context):
    started = time.time()
    run_id = uuid.uuid4().hex[:12]
    bucket = os.environ.get("BUCKET_LAKE") or os.environ.get("DATA_BUCKET")
    namespace = os.environ.get("OBJECT_NAMESPACE")
    places = [p.strip() for p in os.environ.get("PLACES", DEFAULT_PLACES.get(SOURCE, "")).split(",") if p.strip()]

    if not API_KEY:
        raise RuntimeError("API_KEY is not set. Put it in the Environment box when you create the sandbox.")
    if not (bucket and namespace):
        raise RuntimeError("BUCKET_LAKE/DATA_BUCKET and OBJECT_NAMESPACE are set by the sandbox; ask for a bucket.")
    if SOURCE not in FETCHERS:
        raise RuntimeError(f"SOURCE={SOURCE!r}; use one of {', '.join(FETCHERS)}")

    log("run_start", run_id=run_id, source=SOURCE, places=len(places), bucket=bucket)
    rows = FETCHERS[SOURCE](places)
    now = dt.datetime.now(dt.timezone.utc)
    for r in rows:
        r["source"] = SOURCE
        r["run_id"] = run_id
        r["ingested_at"] = now.isoformat()

    body = "\n".join(json.dumps(r, separators=(",", ":")) for r in rows).encode()
    key = f"raw/dt={now:%Y-%m-%d}/hh={now:%H}/{SOURCE}-{now:%Y%m%dT%H%M%S}-{run_id}.jsonl"
    object_storage().put_object(namespace, bucket, key, io.BytesIO(body), content_type="application/x-ndjson")

    dataflow_run = None
    if os.environ.get("TRIGGER_DATAFLOW", "1").strip() not in ("0", "false", "no"):
        try:
            dataflow_run = start_dataflow(run_id)
        except Exception as e:  # noqa: BLE001  the data is written; never fail the ingest for this
            log("dataflow_failed", error=str(e)[:250])

    took = round(time.time() - started, 2)
    log("run_done", run_id=run_id, rows=len(rows), bytes=len(body), object=key,
        dataflow_run=(dataflow_run or "")[-12:], seconds=took)
    return {"rows": len(rows), "object": key, "bucket": bucket, "seconds": took,
            "run_id": run_id, "dataflow_run": dataflow_run}


if __name__ == "__main__":                       # run it locally to check the key
    print(json.dumps(lambda_handler({}, None), indent=2))
    sys.exit(0)
