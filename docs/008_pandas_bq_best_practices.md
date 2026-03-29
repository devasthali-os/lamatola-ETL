# Pandas & BigQuery Best Practices – Feature Extraction and Loading

> Pipeline: `product_recommendation_feature_ingestion`  
> Tasks: `extract_user_events` · `extract_product_catalog` · `transform_features` · `load_feature_store`  
> Stack: pandas 2.x · `pandas-gbq` / `google-cloud-bigquery` · BigQuery  
> Designed for **high-throughput, low-latency** systems at Google / Amazon scale  
> 10B+ events/day · 500M+ entities · < 10 ms online serving SLA  
> Last updated: 2026-03-29

---

## Table of Contents

1. [Reading from BigQuery into pandas](#1-reading-from-bigquery-into-pandas)
2. [Writing from pandas back to BigQuery](#2-writing-from-pandas-back-to-bigquery)
3. [pandas Best Practices for Feature Transformation](#3-pandas-best-practices-for-feature-transformation)
4. [Memory Management](#4-memory-management)
5. [Schema & Data Quality](#5-schema--data-quality)
6. [Performance Patterns](#6-performance-patterns)
7. [Applied to the Feature ETL Pipeline](#7-applied-to-the-feature-etl-pipeline)
8. [Pros and Cons vs Alternatives](#8-pros-and-cons-vs-alternatives)

---

## 1. Reading from BigQuery into pandas

### Two client options

| Client | Import | Best for |
|---|---|---|
| `google-cloud-bigquery` | `from google.cloud import bigquery` | Full control, large results, Arrow backend |
| `pandas-gbq` | `import pandas_gbq` | Quick reads, simple auth, smaller datasets |

### ✅ Use the BigQuery Storage API for large reads (Arrow backend)

The default REST-based export is slow for large tables. The **Storage Read API** streams
data directly into Arrow/pandas — 5–10× faster for datasets > 100K rows.

```python
from google.cloud import bigquery

def extract_user_events(ds: str) -> dict:
    client = bigquery.Client(project="my_project")

    query = f"""
        SELECT
            user_id,
            product_id,
            event_type,
            event_timestamp
        FROM `my_project.events.user_interactions`
        WHERE DATE(event_timestamp) = '{ds}'
    """

    # ✅ create_bqstorage_client=True — uses Storage Read API (fast)
    df = client.query(query).to_dataframe(
        create_bqstorage_client=True,
        dtypes={
            "user_id":         "string",
            "product_id":      "string",
            "event_type":      "category",   # ✅ low-cardinality → category saves RAM
        }
    )

    print(f"[{ds}] Extracted {len(df):,} user events")
    return df
```

### ✅ Always filter on the partition column

```python
# ✅ Prunes partitions — scans only one day's data
WHERE DATE(event_timestamp) = '{ds}'

# ❌ Full table scan — scans all partitions, expensive
WHERE event_timestamp IS NOT NULL
```

### ✅ Select only the columns you need

```python
# ✅ Project only required columns — reduces bytes scanned and memory usage
SELECT user_id, product_id, event_type, event_timestamp
FROM `my_project.events.user_interactions`

# ❌ Avoid SELECT * — pulls all columns including ones not needed for features
SELECT * FROM `my_project.events.user_interactions`
```

### ✅ Use parameterised queries to prevent SQL injection

```python
from google.cloud import bigquery

query = """
    SELECT user_id, product_id, event_type
    FROM `my_project.events.user_interactions`
    WHERE DATE(event_timestamp) = @execution_date
"""

job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("execution_date", "DATE", ds)
    ]
)

df = client.query(query, job_config=job_config).to_dataframe(
    create_bqstorage_client=True
)
```

### ✅ Read product catalogue with `pandas_gbq` for small tables

```python
import pandas_gbq

def extract_product_catalog(ds: str) -> dict:
    query = f"""
        SELECT
            product_id,
            category,
            price,
            brand
        FROM `my_project.catalogue.products`
        WHERE is_active = TRUE
          AND DATE(updated_at) <= '{ds}'
    """

    # ✅ pandas_gbq is simpler for small, static reference tables
    df = pandas_gbq.read_gbq(
        query,
        project_id="my_project",
        progress_bar_type=None,    # suppress progress bar in Airflow logs
        use_bqstorage_api=True,    # still use Storage API for speed
    )

    print(f"[{ds}] Extracted {len(df):,} products")
    return df
```

---

## 2. Writing from pandas back to BigQuery

### ✅ Use `to_gbq` with `if_exists='append'` for daily partitioned loads

```python
import pandas_gbq

def load_feature_store(transformed_df: pd.DataFrame, ds: str) -> None:
    # ✅ Append to the partitioned feature table
    pandas_gbq.to_gbq(
        transformed_df,
        destination_table="my_project.feature_store.product_rec_features",
        project_id="my_project",
        if_exists="append",         # ✅ safe for partitioned tables
        table_schema=[
            {"name": "entity_id",            "type": "STRING"},
            {"name": "feature_date",          "type": "DATE"},
            {"name": "purchase_count_7d",     "type": "INTEGER"},
            {"name": "category_affinity",     "type": "FLOAT"},
            {"name": "popularity_score",      "type": "FLOAT"},
            {"name": "co_view_rate",          "type": "FLOAT"},
            {"name": "ingested_at",           "type": "TIMESTAMP"},
        ],
        progress_bar_type=None,
    )
```

### ✅ Use the BQ client `load_table_from_dataframe` for large writes (Parquet backend)

`to_gbq` uses JSON under the hood for small writes. For large DataFrames, use the
official client which serialises via Parquet — significantly faster and cheaper.

```python
import pandas as pd
from google.cloud import bigquery

def load_feature_store(transformed_df: pd.DataFrame, ds: str) -> None:
    client    = bigquery.Client(project="my_project")
    table_ref = client.dataset("feature_store").table("product_rec_features")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("entity_id",        "STRING",    mode="REQUIRED"),
            bigquery.SchemaField("feature_date",      "DATE",      mode="REQUIRED"),
            bigquery.SchemaField("purchase_count_7d", "INTEGER"),
            bigquery.SchemaField("category_affinity", "FLOAT"),
            bigquery.SchemaField("popularity_score",  "FLOAT"),
            bigquery.SchemaField("co_view_rate",      "FLOAT"),
            bigquery.SchemaField("ingested_at",       "TIMESTAMP"),
        ],
        # ✅ Parquet serialisation — faster and cheaper than JSON
        parquet_compression="SNAPPY",
    )

    job = client.load_table_from_dataframe(
        transformed_df, table_ref, job_config=job_config
    )
    job.result()   # ✅ wait for completion — raises on failure
    print(f"[{ds}] Loaded {job.output_rows:,} rows → feature_store.product_rec_features")
```

### ✅ Upsert with MERGE for idempotent daily reruns

`WRITE_APPEND` will duplicate rows if the DAG is re-run. Use a staging table + `MERGE`
to make `load_feature_store` safe to retry.

```python
def load_feature_store(transformed_df: pd.DataFrame, ds: str) -> None:
    client    = bigquery.Client(project="my_project")
    safe_ds   = ds.replace("-", "")
    stage_ref = f"my_project.feature_store.staging_{safe_ds}"
    target    = "my_project.feature_store.product_rec_features"

    # Step 1 — write to staging (overwrite safe — staging is ephemeral)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        parquet_compression="SNAPPY",
    )
    client.load_table_from_dataframe(
        transformed_df,
        stage_ref,
        job_config=job_config,
    ).result()

    # Step 2 — MERGE staging → target (idempotent upsert)
    client.query(f"""
        MERGE `{target}` T
        USING `{stage_ref}` S
        ON  T.entity_id    = S.entity_id
        AND T.feature_date = S.feature_date
        WHEN MATCHED THEN UPDATE SET
            T.purchase_count_7d  = S.purchase_count_7d,
            T.category_affinity  = S.category_affinity,
            T.popularity_score   = S.popularity_score,
            T.co_view_rate       = S.co_view_rate,
            T.ingested_at        = S.ingested_at
        WHEN NOT MATCHED THEN INSERT ROW
    """).result()

    # Step 3 — drop staging table
    client.delete_table(stage_ref, not_found_ok=True)
    print(f"[{ds}] Upsert complete ✓")
```

---

## 3. pandas Best Practices for Feature Transformation

### ✅ Use vectorised operations — avoid `apply()` row-by-row

```python
# ❌ Slow — Python loop disguised as pandas
df["purchase_count_7d"] = df.groupby("user_id")["event_type"].apply(
    lambda x: (x == "purchase").sum()
)

# ✅ Fast — fully vectorised C-level operation
purchases = df[df["event_type"] == "purchase"]
df_features = (
    purchases
    .groupby("user_id")
    .size()
    .rename("purchase_count_7d")
    .reset_index()
)
```

### ✅ Use `merge` with explicit `how` and `on` — never implicit index joins

```python
# ✅ Explicit, readable, and safe
features_df = user_events_df.merge(
    product_catalog_df[["product_id", "category", "price"]],
    on="product_id",
    how="left",          # keep all user events even if product not in catalogue
    validate="m:1",      # ✅ assert product_id is unique in catalogue — catches data issues
)
```

### ✅ Compute rolling window features with `groupby` + `rolling`

```python
# user_purchase_count_7d — rolling 7-day purchase count per user
df_sorted = df.sort_values(["user_id", "event_timestamp"])

df_sorted["purchase_count_7d"] = (
    df_sorted
    .groupby("user_id")["is_purchase"]                     # 1 if purchase, else 0
    .transform(lambda x: x.rolling("7D", on=df_sorted.loc[x.index, "event_timestamp"]).sum())
)
```

### ✅ Use `pd.Categorical` for low-cardinality string columns

```python
# ✅ category dtype uses integer codes internally — 8–10× less memory than object
df["event_type"] = df["event_type"].astype("category")
df["category"]   = df["category"].astype("category")
```

### ✅ Add `feature_date` and `ingested_at` columns before loading

```python
import pandas as pd
from datetime import datetime, timezone

df["feature_date"] = pd.to_datetime(ds).date()
df["ingested_at"]  = datetime.now(timezone.utc)
```

### ✅ Drop duplicates before loading — guard against upstream double-delivery

```python
# ✅ Deduplicate on the natural key before upsert
df = df.drop_duplicates(subset=["entity_id", "feature_date"], keep="last")
```

---

## 4. Memory Management

### ✅ Downcast numeric columns after extraction

BQ returns `INT64` as `int64` and `FLOAT64` as `float64` by default.
Downcasting to `int32` / `float32` halves memory for most feature values.

```python
def downcast(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes("integer").columns:
        df[col] = pd.to_numeric(df[col], downcast="integer")
    for col in df.select_dtypes("float").columns:
        df[col] = pd.to_numeric(df[col], downcast="float")
    return df

df = downcast(df)
print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1e6:.1f} MB")
```

### ✅ Use chunked reads for very large tables

```python
# ✅ Process in 500K-row chunks to avoid OOM
query = f"SELECT * FROM `my_project.events.user_interactions` WHERE DATE(ts) = '{ds}'"

chunks = []
for chunk in client.query(query).result().to_dataframe_iterable(max_results=500_000):
    chunks.append(process_chunk(chunk))

df = pd.concat(chunks, ignore_index=True)
```

### ✅ Delete intermediate DataFrames explicitly

```python
raw_user_events  = extract_user_events(ds)
product_catalog  = extract_product_catalog(ds)

merged = raw_user_events.merge(product_catalog, on="product_id", how="left")

# ✅ Free memory as soon as the intermediate frame is no longer needed
del raw_user_events, product_catalog
import gc; gc.collect()

features = compute_features(merged)
del merged
gc.collect()
```

---

## 5. Schema & Data Quality

### ✅ Validate schema with Pandera before loading to BQ

```python
import pandera as pa
from pandera import Column, DataFrameSchema, Check

feature_schema = DataFrameSchema({
    "entity_id":           Column(str,   nullable=False),
    "feature_date":        Column("datetime64[ns]", nullable=False),
    "purchase_count_7d":   Column(int,   Check.greater_than_or_equal_to(0)),
    "category_affinity":   Column(float, Check.in_range(0.0, 1.0)),
    "popularity_score":    Column(float, Check.greater_than_or_equal_to(0.0)),
    "co_view_rate":        Column(float, Check.in_range(0.0, 1.0)),
})

def validate_features(df: pd.DataFrame, ds: str) -> pd.DataFrame:
    try:
        validated = feature_schema.validate(df, lazy=True)   # collect all errors, not just first
        print(f"[{ds}] Schema validation passed — {len(validated):,} rows ✓")
        return validated
    except pa.errors.SchemaErrors as e:
        print(e.failure_cases)   # ✅ log all failing rows before raising
        raise ValueError(f"[{ds}] Feature schema validation failed") from e
```

### ✅ Assert row counts are within expected bounds

```python
def assert_row_counts(df: pd.DataFrame, ds: str, min_rows: int = 1000) -> None:
    if len(df) < min_rows:
        raise ValueError(
            f"[{ds}] Only {len(df):,} rows — expected at least {min_rows:,}. "
            "Upstream data may be missing or delayed."
        )
    print(f"[{ds}] Row count check passed: {len(df):,} rows ✓")
```

### ✅ Check for nulls in key columns before loading

```python
def check_nulls(df: pd.DataFrame, key_cols: list[str], ds: str) -> None:
    null_counts = df[key_cols].isnull().sum()
    if null_counts.any():
        raise ValueError(f"[{ds}] Null values found in key columns:\n{null_counts[null_counts > 0]}")
    print(f"[{ds}] Null check passed ✓")

check_nulls(features_df, key_cols=["entity_id", "feature_date"], ds=ds)
```

---

## 6. Performance Patterns

### Read / write performance summary

| Operation | Method | Throughput | Notes |
|---|---|---|---|
| BQ → pandas | `to_dataframe(create_bqstorage_client=True)` | ~1 GB/s | Storage Read API + Arrow |
| BQ → pandas | `pandas_gbq.read_gbq()` (default) | ~50 MB/s | REST export — slow for large tables |
| pandas → BQ | `load_table_from_dataframe` (Parquet) | ~500 MB/s | Recommended for > 10K rows |
| pandas → BQ | `pandas_gbq.to_gbq()` | ~100 MB/s | JSON serialisation — fine for small loads |
| pandas → BQ | Streaming insert (`insert_rows_json`) | — | ❌ Avoid for batch ETL — costly and not deduped |

### ✅ Avoid streaming inserts for batch ETL

```python
# ❌ Streaming insert — charged per row, not deduplicated, not ideal for batch
client.insert_rows_json(table_ref, rows)

# ✅ Use load job (Parquet) — bulk, cheap, transactional
client.load_table_from_dataframe(df, table_ref, job_config=job_config).result()
```

### ✅ Use PyArrow types for BQ → pandas schema control

```python
import pyarrow as pa
from google.cloud.bigquery_storage_v1 import types

# ✅ Explicitly map BQ types to Arrow types for precision control
arrow_schema = pa.schema([
    pa.field("user_id",       pa.string()),
    pa.field("product_id",    pa.string()),
    pa.field("price",         pa.float32()),    # float32 not float64 — saves memory
    pa.field("event_ts",      pa.timestamp("us", tz="UTC")),
])
```

---

## 7. Applied to the Feature ETL Pipeline

Full implementation of all four DAG tasks using the patterns above:

```python
# dags/product_recs_feature.py  — production-ready task implementations

import gc
import pandas as pd
import pandera as pa
from datetime import datetime, timezone
from google.cloud import bigquery
import pandas_gbq
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'ml-platform',
    'start_date': days_ago(1),
    'email': ['ml-platform@example.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

PROJECT   = "my_project"
BQ_EVENTS = f"{PROJECT}.events.user_interactions"
BQ_CATALOG= f"{PROJECT}.catalogue.products"
BQ_FEATURES = f"{PROJECT}.feature_store.product_rec_features"

feature_schema = pa.DataFrameSchema({
    "entity_id":          pa.Column(str,   nullable=False),
    "feature_date":       pa.Column("datetime64[ns]"),
    "purchase_count_7d":  pa.Column(int,   pa.Check.ge(0)),
    "category_affinity":  pa.Column(float, pa.Check.in_range(0.0, 1.0)),
    "popularity_score":   pa.Column(float, pa.Check.ge(0.0)),
    "co_view_rate":       pa.Column(float, pa.Check.in_range(0.0, 1.0)),
})

@dag(
    dag_id='product_recommendation_feature_ingestion',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['ml', 'feature-ingestion', 'product-recommendation'],
)
def product_recommendation_feature_ingestion():

    @task
    def extract_user_events(ds: str) -> dict:
        client = bigquery.Client(project=PROJECT)
        query  = """
            SELECT user_id, product_id, event_type, event_timestamp
            FROM `{table}`
            WHERE DATE(event_timestamp) = @ds
        """.format(table=BQ_EVENTS)

        df = client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("ds", "DATE", ds)]
            )
        ).to_dataframe(create_bqstorage_client=True,
                       dtypes={"event_type": "category"})

        df["event_type"] = df["event_type"].astype("category")
        df = pd.to_numeric(df.select_dtypes("integer"), downcast="integer", errors="ignore")

        path = f"/tmp/features/user_events_{ds}.parquet"
        df.to_parquet(path, index=False, compression="snappy")

        assert len(df) > 0, f"[{ds}] user_events is empty"
        print(f"[{ds}] Extracted {len(df):,} user events → {path}")
        return {"path": path, "rows": len(df), "date": ds}

    @task
    def extract_product_catalog(ds: str) -> dict:
        df = pandas_gbq.read_gbq(
            f"""
            SELECT product_id, category, price, brand
            FROM `{BQ_CATALOG}`
            WHERE is_active = TRUE AND DATE(updated_at) <= '{ds}'
            """,
            project_id=PROJECT,
            use_bqstorage_api=True,
            progress_bar_type=None,
            dtypes={"category": "category"},
        )

        path = f"/tmp/features/product_catalog_{ds}.parquet"
        df.to_parquet(path, index=False, compression="snappy")

        assert len(df) > 0, f"[{ds}] product_catalog is empty"
        print(f"[{ds}] Extracted {len(df):,} products → {path}")
        return {"path": path, "rows": len(df), "date": ds}

    @task
    def validate_features(user_events: dict, product_catalog: dict) -> dict:
        ds = user_events["date"]
        if user_events["rows"] == 0:
            raise ValueError(f"[{ds}] user_events is empty")
        if product_catalog["rows"] == 0:
            raise ValueError(f"[{ds}] product_catalog is empty")
        print(f"[{ds}] Validation passed ✓")
        return {"user_events": user_events, "product_catalog": product_catalog}

    @task
    def transform_features(validated: dict) -> dict:
        ds      = validated["user_events"]["date"]
        ue_df   = pd.read_parquet(validated["user_events"]["path"])
        pc_df   = pd.read_parquet(validated["product_catalog"]["path"])

        # Merge
        merged = ue_df.merge(
            pc_df[["product_id", "category", "price"]],
            on="product_id", how="left", validate="m:1",
        )

        # Derived features
        features = (
            merged.groupby("user_id")
            .agg(
                purchase_count_7d=("event_type", lambda x: (x == "purchase").sum()),
                category_affinity=("category",   lambda x: x.value_counts(normalize=True).max()),
            )
            .reset_index()
            .rename(columns={"user_id": "entity_id"})
        )

        product_features = (
            merged.groupby("product_id")
            .agg(
                popularity_score=("event_type", "count"),
                co_view_rate=("user_id", pd.Series.nunique),
            )
            .reset_index()
            .rename(columns={"product_id": "entity_id"})
        )

        # Normalise scores to [0, 1]
        for col in ["popularity_score", "co_view_rate"]:
            mx = product_features[col].max()
            product_features[col] = (product_features[col] / mx).clip(0, 1) if mx else 0

        all_features = pd.concat([features, product_features], ignore_index=True)
        all_features["feature_date"] = pd.to_datetime(ds)
        all_features["ingested_at"]  = datetime.now(timezone.utc)

        del merged, ue_df, pc_df; gc.collect()

        # Validate schema before writing
        feature_schema.validate(all_features, lazy=True)

        path = f"/tmp/features/transformed_{ds}.parquet"
        all_features.to_parquet(path, index=False, compression="snappy")
        print(f"[{ds}] Transformed {len(all_features):,} feature rows → {path}")
        return {"path": path, "rows": len(all_features), "date": ds}

    @task
    def load_feature_store(transformed: dict) -> None:
        ds      = transformed["date"]
        safe_ds = ds.replace("-", "")
        df      = pd.read_parquet(transformed["path"])
        client  = bigquery.Client(project=PROJECT)

        # Step 1 — load to staging
        stage = f"{PROJECT}.feature_store.staging_{safe_ds}"
        client.load_table_from_dataframe(
            df, stage,
            job_config=bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                parquet_compression="SNAPPY",
            )
        ).result()

        # Step 2 — MERGE into target (idempotent)
        client.query(f"""
            MERGE `{BQ_FEATURES}` T
            USING `{stage}` S
            ON T.entity_id = S.entity_id AND T.feature_date = S.feature_date
            WHEN MATCHED THEN UPDATE SET
                T.purchase_count_7d = S.purchase_count_7d,
                T.category_affinity = S.category_affinity,
                T.popularity_score  = S.popularity_score,
                T.co_view_rate      = S.co_view_rate,
                T.ingested_at       = S.ingested_at
            WHEN NOT MATCHED THEN INSERT ROW
        """).result()

        # Step 3 — clean up staging
        client.delete_table(stage, not_found_ok=True)
        print(f"[{ds}] Loaded {transformed['rows']:,} rows → {BQ_FEATURES} ✓")

    user_events     = extract_user_events()
    product_catalog = extract_product_catalog()
    validated       = validate_features(user_events, product_catalog)
    transformed     = transform_features(validated)
    load_feature_store(transformed)

product_recommendation_feature_ingestion()
```

---

## 8. Pros and Cons vs Alternatives

| Dimension | pandas + BQ client | Spark / Dataproc | dbt | BQ-native (SQL only) |
|---|---|---|---|---|
| **Ease of use** | ✅ Familiar to all Python devs | ❌ Steep learning curve | ✅ SQL-native, easy for analysts | ✅ Pure SQL |
| **Scale** | ⚠️ Single-node RAM-limited | ✅ Distributed, petabyte-scale | ✅ Runs inside BQ — serverless | ✅ Runs inside BQ — serverless |
| **Feature logic flexibility** | ✅ Full Python — any library | ✅ Full Python / Scala / Java | ⚠️ SQL + Jinja macros only | ⚠️ SQL only |
| **Rolling windows** | ✅ `groupby` + `rolling` | ✅ `Window` functions | ⚠️ Manual SQL window functions | ✅ BQ native `OVER()` |
| **Memory control** | ✅ Explicit downcasting + chunking | ✅ Distributed partitions | N/A — runs in BQ | N/A — runs in BQ |
| **BQ read cost** | ✅ Storage API — low | ✅ Storage API — low | ✅ Runs inside BQ — no egress | ✅ No egress |
| **Validation** | ✅ Pandera / Great Expectations | ✅ Great Expectations | ✅ dbt tests | ⚠️ Manual SQL assertions |
| **Iteration speed** | ✅ Fast local dev with `df.head()` | ❌ Slow cluster startup | ✅ Fast with `dbt run` | ✅ Fast in BQ console |
| **Infra required** | ✅ Just a Python process | ❌ Dataproc cluster | ✅ None (runs in BQ) | ✅ None |
| **Best for** | < 500M rows, complex Python logic | > 500M rows, ML-heavy transforms | SQL-centric analytics features | Simple aggregations in BQ |

---

## 9. High-Throughput & Low-Latency Scale Patterns

### When to move beyond pandas

| Daily event volume | Recommended approach |
|---|---|
| < 10 GB | pandas + BQ Storage API (current) |
| 10–100 GB | pandas + chunked reads + Parquet load job |
| 100 GB–1 TB | Apache Beam (Dataflow) with autoscaling |
| > 1 TB | Spark on Dataproc or Dataflow with 100+ workers |

### ✅ Shard BQ extraction across hourly partitions

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import bigquery

def extract_hour(ds: str, hour: int) -> pd.DataFrame:
    client = bigquery.Client(project="my_project")
    query  = f"""
        SELECT user_id, product_id, event_type, event_timestamp
        FROM `my_project.events.user_interactions`
        WHERE DATE(event_timestamp) = '{ds}'
          AND EXTRACT(HOUR FROM event_timestamp) = {hour}
    """
    return client.query(query).to_dataframe(create_bqstorage_client=True)

def extract_user_events(ds: str) -> pd.DataFrame:
    # ✅ Fetch all 24 hours in parallel using a thread pool
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(extract_hour, ds, h): h for h in range(24)}
        chunks  = [f.result() for f in as_completed(futures)]
    df = pd.concat(chunks, ignore_index=True)
    print(f"[{ds}] Extracted {len(df):,} events across 24 hours")
    return df
```

### ✅ Write to BQ via GCS intermediate for maximum throughput

Direct `load_table_from_dataframe` serialises in Python. For very large DataFrames,
write Parquet to GCS first then trigger a BQ load job — 3–5× faster.

```python
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery, storage

def load_feature_store_fast(df: pd.DataFrame, ds: str) -> None:
    safe_ds = ds.replace("-", "")
    gcs_uri = f"gs://ml-feature-ingestion-bucket/staging/{safe_ds}/features.parquet"

    # Step 1 — write Parquet directly to GCS via PyArrow (no local disk)
    gcs_client   = storage.Client()
    bucket, blob = gcs_uri[5:].split("/", 1)
    with gcs_client.bucket(bucket).blob(blob).open("wb") as f:
        pq.write_table(pa.Table.from_pandas(df), f, compression="snappy")

    # Step 2 — BQ load job from GCS (runs inside Google infra — no egress billing)
    bq_client  = bigquery.Client(project="my_project")
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        parquet_options=bigquery.ParquetOptions(enable_list_inference=True),
    )
    bq_client.load_table_from_uri(
        gcs_uri,
        f"my_project.feature_store.staging_{safe_ds}",
        job_config=job_config,
    ).result()

    # Step 3 — MERGE (idempotent upsert) — runs inside BQ, no data movement
    bq_client.query(f"""
        MERGE `my_project.feature_store.product_rec_features` T
        USING `my_project.feature_store.staging_{safe_ds}` S
        ON T.entity_id = S.entity_id AND T.feature_date = S.feature_date
        WHEN MATCHED     THEN UPDATE SET T.purchase_count_7d = S.purchase_count_7d, ...
        WHEN NOT MATCHED THEN INSERT ROW
    """).result()
```

### ✅ Use BigQuery `INFORMATION_SCHEMA` to monitor write throughput

```sql
-- Check bytes written and slot usage for the last 24 feature ingestion jobs
SELECT
    job_id,
    creation_time,
    total_bytes_processed / POW(1024, 3)  AS gb_processed,
    total_slot_ms / 1000                  AS slot_seconds,
    TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration_sec,
    state,
    error_result.message                  AS error
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND statement_type IN ('INSERT', 'MERGE', 'LOAD')
  AND destination_table.dataset_id = 'feature_store'
ORDER BY creation_time DESC
LIMIT 20;
```

### ✅ Use BQ materialized views to pre-aggregate features for the online store

```sql
-- ✅ Materialized view — BQ auto-refreshes within minutes of base table update
-- Serving layer reads from this view instead of scanning the full feature table
CREATE MATERIALIZED VIEW `my_project.feature_store.product_rec_features_latest`
OPTIONS (enable_refresh = true, refresh_interval_minutes = 30)
AS
SELECT
    entity_id,
    feature_date,
    purchase_count_7d,
    category_affinity,
    popularity_score,
    co_view_rate
FROM `my_project.feature_store.product_rec_features`
WHERE feature_date = CURRENT_DATE()
;
```

### ✅ Use `pandas` + `pyarrow` for efficient BQ → Redis materialisation

```python
import pyarrow.parquet as pq
import redis

def materialise_to_redis(gcs_parquet_path: str, ds: str) -> None:
    # ✅ Read Parquet directly from GCS into Arrow — no pandas conversion overhead
    table  = pq.read_table(gcs_parquet_path, filesystem=gcsfs.GCSFileSystem())
    df     = table.to_pandas(types_mapper=pd.ArrowDtype)

    r    = redis.RedisCluster(startup_nodes=[...], decode_responses=True)
    pipe = r.pipeline(transaction=False)

    BATCH = 10_000
    for i, (_, row) in enumerate(df.iterrows()):
        pipe.hset(f"user:{row['entity_id']}:features", mapping={
            "purchase_count_7d": int(row["purchase_count_7d"]),
            "category_affinity": float(row["category_affinity"]),
            "popularity_score":  float(row["popularity_score"]),
            "co_view_rate":      float(row["co_view_rate"]),
            "feature_date":      ds,
        })
        pipe.expire(f"user:{row['entity_id']}:features", 172800)   # 48h TTL
        if i % BATCH == 0 and i > 0:
            pipe.execute()
            pipe = r.pipeline(transaction=False)
    pipe.execute()
    print(f"[{ds}] Materialised {len(df):,} entities to Redis ✓")

