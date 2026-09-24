# Energy Demand & Price Forecasting — ETL Pipeline

An end-to-end data engineering pipeline that ingests real-time electricity
demand data from the US EIA (Energy Information Administration) API, processes
it through a Bronze/Silver/Gold medallion architecture on Azure Databricks, and
surfaces business-level insights for grid capacity planning.

## Business problem

A utility operations team needs a daily, region-level view of electricity
demand trends to plan grid load and identify peak-usage patterns. This
pipeline automates the collection and transformation of hourly demand data
across four major US grid regions (California, Texas, New York, PJM) into
analysis-ready tables.

## Architecture

```
EIA API (api.eia.gov)
        │
        ├──────────────────────┐
        ▼                      ▼
  Python script          Azure Data Factory
  (Databricks)            (REST connector,
                          daily scheduled)
        │                      │
        └──────────┬───────────┘
                    ▼
          ADLS Gen2 — Bronze
          (raw CSV/JSON, date-partitioned)
                    │
                    ▼
       Databricks — Silver
   (typed, deduplicated, UTC-normalized)
                    │
                    ▼
        Databricks — Gold
   (region summaries, peak-hour analysis)
```

Two independent, working ingestion paths were built deliberately, to compare
approaches:
1. **Python on Databricks** — explicit pagination, checkpoint-based incremental
   loading, credentials via Databricks Secrets backed by Azure Key Vault.
2. **Azure Data Factory** — REST connector, no-code data mapping, daily
   Schedule Trigger with dynamic date-window parameters.

Two transformation approaches were also built:
1. **Hand-written PySpark batch notebook** — explicit read/transform/write,
   full control over every step.
2. **Lakeflow Declarative Pipelines (formerly Delta Live Tables)** — declarative
   table definitions, automatic dependency resolution, built-in data quality
   enforcement via `@dlt.expect_or_drop`.

## Key engineering decisions

- **Storage authentication:** Access Connector for Azure Databricks (managed
  identity) rather than storage account keys — no long-lived secrets in code
  or config.
- **Secret management:** EIA API key stored in Azure Key Vault, accessed via a
  Key Vault-backed Databricks secret scope — never hardcoded.
- **Timezone handling:** confirmed via EIA's own documentation that
  `frequency=hourly` returns UTC timestamps (not per-region local time),
  which avoids daylight-saving-time ambiguity when comparing regions.
- **Deduplication key:** `(period, respondent, type)` — the natural key for
  "one region's one measurement type at one hour," guarding against
  double-ingestion from overlapping manual test runs and the scheduled
  pipeline.
- **CSV vs JSON in Bronze:** the two ingestion paths intentionally produce
  different raw formats (ADF → CSV, Databricks script → JSON), reflecting a
  realistic scenario where different source systems land data differently;
  the Silver layer normalizes both into one schema.

## Known limitations (honest, not hidden)

- **Incremental loading is schedule-based, not watermark-based.** Each run
  looks back exactly one day from trigger time. If a run fails, that day's
  data is skipped rather than automatically retried on the next run. A
  production version would add a watermark table tracking last-successful-run
  state.
- **Data quality checks are minimal** (`value IS NOT NULL`). A production
  version would validate value ranges, expected region codes, and timestamp
  continuity.
- **Currently Demand-only** (`type=D`). The EIA route also supports Net
  Generation (`type=NG`) and Total Interchange (`type=TI`), which would enrich
  Gold-layer analysis (e.g., demand vs. generation balance per region) — noted
  as a natural next iteration rather than built, to keep v1 scope focused.


## Sample output

Gold-layer daily demand summary (real data, four regions):

| Region | Hours Recorded | Avg Demand (MWh) | Peak Demand (MWh) |
|---|---|---|---|
| PJM | 25 | 90,426 | 99,703 |
| TEX | 25 | 69,798 | 85,290 |
| CAL | 25 | 34,366 | 38,980 |
| NYIS | 25 | 15,125 | 18,250 |

These numbers pass a real-world sanity check: PJM (covering a large portion of
the US mid-Atlantic/Midwest) shows the highest demand, while NYIS (New York
state only) shows the lowest — consistent with each region's actual grid size.

## Tech stack

Python, PySpark, Azure Databricks, Azure Data Factory, Azure Data Lake Storage
Gen2, Delta Lake, Lakeflow Declarative Pipelines, Azure Key Vault, Unity
Catalog, EIA Open Data API.

## Possible extensions

- Add weather data (Open-Meteo) to correlate demand spikes with temperature
- Add Net Generation and Total Interchange measurement types
- Replace schedule-based incrementality with a true watermark table
- Power BI dashboard via Databricks SQL Warehouse connection
- Migrate Gold tables to a dedicated warehouse (e.g. Azure Synapse) if BI
  workload demands guaranteed low-latency SQL separate from the lakehouse
