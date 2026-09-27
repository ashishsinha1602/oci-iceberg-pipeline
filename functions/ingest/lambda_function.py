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

    took = round(time.time() - started, 2)
    log("run_done", run_id=run_id, rows=len(rows), bytes=len(body), object=key, seconds=took)
    return {"rows": len(rows), "object": key, "bucket": bucket, "seconds": took, "run_id": run_id}


if __name__ == "__main__":                       # run it locally to check the key
    print(json.dumps(lambda_handler({}, None), indent=2))
    sys.exit(0)
