# Product Recs DAG
Feature Ingestion Pipeline for Product Recommendation

## Overview

| Attribute         | Value                                                      |
|-------------------|------------------------------------------------------------|
| **DAG ID**        | `product_recommendation_feature_ingestion`                 |
| **Owner**         | airflow                                                    |
| **Schedule**      | Daily (`@daily`)                                           |
| **Start Date**    | 2 days ago (relative)                                      |
| **Retries**       | 1 (retry delay: 5 minutes)                                 |
| **Operator Type** | `BashOperator`                                             |
| **Purpose**       | Ingest & transform product/user features for recommendation engine |

---

## DAG Flow

```mermaid
flowchart TD
    A([🚀 DAG Start\nproduct_recommendation_feature_ingestion]) --> T1

    T1["🖨️ print_date\n─────────────\nOperator: BashOperator\nCommand: date\nLogs ingestion run timestamp"]

    T1 --> T2
    T1 --> T3

    T2["💤 sleep\n─────────────\nOperator: BashOperator\nCommand: sleep 5\nSimulates feature extraction delay"]

    T3["📄 templated\n─────────────\nOperator: BashOperator\nCommand: Jinja template\nIterates 5x, echoes\nds, ds+7, my_param"]

    T2 --> END
    T3 --> END

    END([✅ DAG End])

    style A fill:#4CAF50,color:#fff,stroke:#388E3C
    style T1 fill:#2196F3,color:#fff,stroke:#1565C0
    style T2 fill:#FF9800,color:#fff,stroke:#E65100
    style T3 fill:#9C27B0,color:#fff,stroke:#6A1B9A
    style END fill:#4CAF50,color:#fff,stroke:#388E3C
```

---

## Dependency Graph (simplified)

```mermaid
graph LR
    print_date --> sleep
    print_date --> templated
```

---

## Retry & Alerting Policy

```mermaid
flowchart LR
    RUN[Task Runs] --> SUCCESS{Success?}
    SUCCESS -- Yes --> DONE[Mark Success]
    SUCCESS -- No --> RETRY{Retries\nremaining?}
    RETRY -- Yes\n wait 5 min --> RUN
    RETRY -- No --> FAIL[Mark Failed\n Send email alert]
```
