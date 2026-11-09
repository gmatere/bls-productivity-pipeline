# BLS Productivity Pipeline

US labour productivity time series (Bureau of Labor Statistics) and national
population (Data USA), ingested and modelled on Databricks.

- **Source** — all files under the BLS `pr/` time-series folder, discovered at
  runtime, plus the Data USA population API. Landed raw in a Unity Catalog Volume.
- **Model** — Bronze / Silver / Gold as a Spark Declarative Pipeline, with
  expectations enforcing schema and key integrity.

## Answers

_Filled in once the Gold layer lands._

## Layout

| Path | |
|---|---|
| `notebooks/01_ingest.py` | manifest-driven sourcing |
| `notebooks/02_pipeline.py` | Bronze / Silver / Gold definitions |
| `notebooks/03_alternatives.py` | second implementation of each question |
| `PROCESS.md` | architecture, trade-offs, retrospective |
| `screenshots/` | pipeline and table evidence |
