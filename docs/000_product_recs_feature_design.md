# Product Recommendation – Feature Ingestion Pipeline

> Designed for **high-throughput, low-latency** systems at Google / Amazon scale:  
> billions of user events · hundreds of millions of products · < 10 ms online serving SLA

## Overview

| Attribute           | Value                                                                              |
|---------------------|------------------------------------------------------------------------------------|
| **DAG ID**          | `product_recommendation_feature_ingestion`                                         |
| **File**            | `dags/product_recs_feature.py`                                                     |
| **Owner**           | ml-platform                                                                        |
| **Schedule**        | `0 2 * * *` — daily at 02:00 UTC                                                   |
| **Catchup**         | `False`                                                                            |
| **Max Active Runs** | 1                                                                                  |
| **Retries**         | 2 (retry delay: 5 minutes)                                                         |
| **Tags**            | `ml`, `feature-ingestion`, `product-recommendation`                                |
| **Operator Type**   | `@task` decorator (`PythonOperator`)                                               |
| **Target scale**    | 10B+ events/day · 500M+ entities · sub-10ms online feature serving                |
| **Purpose**         | Ingest user events & product catalogue → validate → transform → load feature store |

---

## DAG Flow

```mermaid
flowchart TD
    A([🚀 DAG Start\n02:00 UTC daily]) --> T1 & T2

    T1["📥 extract_user_events\n───────────────────\nPulls raw user interaction\nevents for execution date\nclicks · views · purchases\nOutput: user_events_ds.parquet"]

    T2["📦 extract_product_catalog\n───────────────────\nPulls product catalogue snapshot\nid · category · price · metadata\nOutput: product_catalog_ds.parquet"]

    T1 --> T3
    T2 --> T3

    T3["✅ validate_features\n───────────────────\nSchema + data quality checks\n• Non-empty datasets\n• No null user_id / product_id\n• Price values > 0"]

    T3 --> T4

    T4["⚙️ transform_features\n───────────────────\nJoins events + catalogue\nComputes derived features:\n• user_purchase_count_7d\n• user_category_affinity\n• product_popularity_score\n• product_co_view_rate\nOutput: transformed_ds.parquet"]

    T4 --> T5

    T5["💾 load_feature_store\n───────────────────\nUpserts features into store\nKeyed by entity_id + feature_date\nIdempotent — safe to re-run"]

    T5 --> END([✅ DAG End])

    style A    fill:#4CAF50,color:#fff,stroke:#388E3C
    style T1   fill:#2196F3,color:#fff,stroke:#1565C0
    style T2   fill:#2196F3,color:#fff,stroke:#1565C0
    style T3   fill:#FF9800,color:#fff,stroke:#E65100
    style T4   fill:#9C27B0,color:#fff,stroke:#6A1B9A
    style T5   fill:#00897B,color:#fff,stroke:#00695C
    style END  fill:#4CAF50,color:#fff,stroke:#388E3C
```

---

## Task Details

| Task | Runs after | What it does | Output |
|---|---|---|---|
| `extract_user_events` | DAG start | Fetches raw click/view/purchase events for `ds` | `user_events_{ds}.parquet` |
| `extract_product_catalog` | DAG start | Fetches product catalogue snapshot for `ds` | `product_catalog_{ds}.parquet` |
| `validate_features` | Both extract tasks | Schema + nullability + range checks | Validated metadata dict |
| `transform_features` | validate | Joins datasets, computes 4 derived features | `transformed_{ds}.parquet` |
| `load_feature_store` | transform | Upserts into feature store by `entity_id + feature_date` | — |

---

## Dependency Graph

```mermaid
graph LR
    extract_user_events --> validate_features
    extract_product_catalog --> validate_features
    validate_features --> transform_features
    transform_features --> load_feature_store
```

---

## Data Flow

```mermaid
flowchart LR
    SRC1["🗄️ Source\nUser Events DB\nKafka / S3"] --> EUE[extract_user_events]
    SRC2["🗄️ Source\nProduct Catalogue\nDB / API"] --> EPC[extract_product_catalog]
    EUE --> VAL[validate_features]
    EPC --> VAL
    VAL --> TRF[transform_features]
    TRF --> FS["🏪 Feature Store\nFeast / Redis\nPostgreSQL"]
    FS --> REC["🤖 Recommendation\nEngine"]
```

---

## Retry & Alerting Policy

```mermaid
flowchart LR
    RUN[Task Runs] --> OK{Success?}
    OK -- Yes --> DONE[✅ Mark Success]
    OK -- No --> CHK{Retries left?\nmax=2}
    CHK -- Yes\nwait 5 min --> RUN
    CHK -- No --> FAIL[❌ Mark Failed]
    FAIL --> ALERT[📧 Email Alert\nml-platform@example.com]
```

---

## Scale Considerations (Google / Amazon Scale)

| Dimension | Threshold | Recommended approach |
|---|---|---|
| **Event volume** | > 1B events/day | Partition BQ table by hour, not day; use Dataflow/Spark for transform |
| **Entity count** | > 500M users or products | Bigtable online store over Redis (RAM cost); shard BQ staging tables |
| **Feature serving SLA** | < 10 ms P99 | Redis Cluster / Memorystore with read replicas; avoid cold paths |
| **Feature serving SLA** | < 1 ms P99 | In-process feature cache (e.g. local LRU) in the serving container |
| **Pipeline throughput** | > 100 GB/day | Replace pandas with Dataflow (Apache Beam) or Spark on Dataproc |
| **Backfill speed** | Years of history | Parallel Airflow `max_active_runs > 1` with sharded date ranges |
| **Online store write throughput** | > 100K writes/s | Redis Cluster with pipeline batching; Bigtable with bulk mutations |
| **Training data freshness** | Near-realtime | Add a streaming path (Pub/Sub → Dataflow → Bigtable) alongside the batch DAG |

### Production-scale pipeline shape

```mermaid
flowchart TD
    subgraph Batch["⚙️ Batch Path — Airflow DAG (daily 02:00 UTC)"]
        E1["extract_user_events\nBQ → GCS parquet\n~100 GB/day"] & E2["extract_product_catalog\nBQ snapshot"] --> V["validate_features\nPandera + row count checks"]
        V --> T["transform_features\nDataflow / Spark\nhorizontally scalable"]
        T --> L["load_feature_store\nBQ MERGE + Redis pipeline\nor Bigtable bulk mutations"]
    end

    subgraph Stream["⚡ Streaming Path (near-realtime)"]
        PS["Pub/Sub\nuser event stream"] --> DF["Dataflow\nstreaming pipeline"] --> BT["Bigtable / Redis\nonline store"]
    end

    L --> FS_OFF["BigQuery\nOffline Store\npoint-in-time training data"]
    L --> FS_ON["Redis / Bigtable\nOnline Store\n< 10ms serving"]
    BT --> FS_ON

    FS_OFF --> TRAIN["Vertex AI / SageMaker\nModel Training"]
    FS_ON  --> SERVE["Recommendation API\n< 10ms P99 · 100K rps"]
```

