# Databricks notebook source
#config and a resilient session

import hashlib, re, time, requests
from datetime import datetime, timezone
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE     = "https://download.bls.gov/pub/time.series/pr/"
PREFIX   = "/pub/time.series/pr/"
VOLUME   = "/Volumes/workspace/rearc/raw"
MANIFEST = "workspace.rearc.ingestion_manifest"

# BLS policy (https://www.bls.gov/bls/pss.htm) asks automated clients to
# identify themselves with contact info, else they reserve the right to block.
HEADERS = {"User-Agent": "gmatere.t@gmail.com (data engineering exercise)"}

def make_session() -> requests.Session:
    """Back off on rate limiting and transient server errors rather than
    hammering a public government endpoint."""
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.5,                     # 1.5s, 3s, 6s, 12s, 24s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        respect_retry_after_header=True,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update(HEADERS)
    return s

session = make_session()

# COMMAND ----------

#the manifest table

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {MANIFEST} (
  filename       STRING NOT NULL,
  source_url     STRING,
  etag           STRING,
  last_modified  STRING,
  content_sha256 STRING,
  size_bytes     BIGINT,
  first_seen_at  TIMESTAMP,
  ingested_at    TIMESTAMP,
  status         STRING
) USING DELTA
""")

# COMMAND ----------

#discovery

def list_source_files() -> list[str]:
    r = session.get(BASE, timeout=30)
    r.raise_for_status()
    hrefs = re.findall(r'<a href="([^"]+)"', r.text, flags=re.I)
    return sorted({h.rsplit("/", 1)[-1] for h in hrefs
                   if h.startswith(PREFIX) and h != PREFIX})

# COMMAND ----------

# the sync

def current_manifest() -> dict:
    rows = spark.table(MANIFEST).collect()
    return {r["filename"]: r.asDict() for r in rows}

def sync() -> list[dict]:
    """Returns one audit row per file describing what happened and why."""
    now, log = datetime.now(timezone.utc), []
    files, known = list_source_files(), current_manifest()

    for fn in files:
        url  = BASE + fn
        head = session.head(url, timeout=30)
        etag, lastmod = head.headers.get("ETag"), head.headers.get("Last-Modified")
        prior = known.get(fn)

        # Cheap path: headers unchanged since last ingest, nothing to do.
        if prior and prior["status"] == "active" \
           and prior["etag"] == etag and prior["last_modified"] == lastmod:
            log.append({"filename": fn, "action": "skipped", "reason": "headers unchanged"})
            continue

        body   = session.get(url, timeout=120).content
        digest = hashlib.sha256(body).hexdigest()

        # Headers moved but content didn't. BLS ETags are batch-derived, not
        # content hashes, so this happens on republish. Don't re-land the file.
        if prior and prior["content_sha256"] == digest:
            spark.sql(f"""UPDATE {MANIFEST}
                          SET etag='{etag}', last_modified='{lastmod}'
                          WHERE filename='{fn}'""")
            log.append({"filename": fn, "action": "metadata_only",
                        "reason": "headers changed, content identical"})
            continue

        with open(f"{VOLUME}/{fn}", "wb") as f:
            f.write(body)

        spark.sql(f"""
          MERGE INTO {MANIFEST} t
          USING (SELECT '{fn}' AS filename) s ON t.filename = s.filename
          WHEN MATCHED THEN UPDATE SET
            etag='{etag}', last_modified='{lastmod}', content_sha256='{digest}',
            size_bytes={len(body)}, ingested_at=TIMESTAMP'{now}', status='active'
          WHEN NOT MATCHED THEN INSERT
            (filename, source_url, etag, last_modified, content_sha256,
             size_bytes, first_seen_at, ingested_at, status)
            VALUES ('{fn}','{url}','{etag}','{lastmod}','{digest}',
                    {len(body)}, TIMESTAMP'{now}', TIMESTAMP'{now}', 'active')
        """)
        log.append({"filename": fn,
                    "action": "ingested" if not prior else "reingested",
                    "reason": "new file" if not prior else "content changed"})
        time.sleep(0.5)   # be a polite client

    # Tombstone only files this function manages. population.json comes from a
    # different source and is not in the BLS listing, so an unscoped
    # set-difference would mark it removed on every run.
    gone = {fn for fn, row in known.items()
            if (row["source_url"] or "").startswith(BASE) and fn not in files}
    for fn in gone:
        if known[fn]["status"] == "active":
            spark.sql(f"UPDATE {MANIFEST} SET status='removed' WHERE filename='{fn}'")
            log.append({"filename": fn, "action": "tombstoned",
                        "reason": "no longer present at source"})
    return log

# COMMAND ----------

#final run

for row in sync():
    print(f"{row['action']:<14} {row['filename']:<22} {row['reason']}")

# COMMAND ----------

POP_URL = ("https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
           "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation"
           "&locale=en&measures=Population")
POP_FILE = "population.json"

resp = session.get(POP_URL, timeout=60)
resp.raise_for_status()
body = resp.content

# Land the response exactly as returned. Parsing belongs in Bronze, not here.
with open(f"{VOLUME}/{POP_FILE}", "wb") as f:
    f.write(body)

# Same manifest, same change-detection story. This API sends no ETag, so the
# content hash is the only signal available - which is why the hash is the
# authority rather than the headers.
now    = datetime.now(timezone.utc)
digest = hashlib.sha256(body).hexdigest()
etag   = resp.headers.get("ETag")
lastmod = resp.headers.get("Last-Modified")

spark.sql(f"""
  MERGE INTO {MANIFEST} t
  USING (SELECT '{POP_FILE}' AS filename) s ON t.filename = s.filename
  WHEN MATCHED THEN UPDATE SET
    etag={'NULL' if etag is None else f"'{etag}'"},
    last_modified={'NULL' if lastmod is None else f"'{lastmod}'"},
    content_sha256='{digest}', size_bytes={len(body)},
    ingested_at=TIMESTAMP'{now}', status='active'
  WHEN NOT MATCHED THEN INSERT
    (filename, source_url, etag, last_modified, content_sha256,
     size_bytes, first_seen_at, ingested_at, status)
    VALUES ('{POP_FILE}', '{POP_URL}',
            {'NULL' if etag is None else f"'{etag}'"},
            {'NULL' if lastmod is None else f"'{lastmod}'"},
            '{digest}', {len(body)}, TIMESTAMP'{now}', TIMESTAMP'{now}', 'active')
""")

print(f"landed {POP_FILE}  {len(body):,} bytes  sha256={digest[:12]}…")
display(spark.table(MANIFEST).select("filename","size_bytes","status").orderBy("filename"))

# COMMAND ----------

