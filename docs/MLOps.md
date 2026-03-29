# MLOps Infrastructure

## Overview

The pipeline runs on a **local Docker-based Airflow 2.8.1** stack orchestrated via `docker-compose`. It uses a `LocalExecutor` backed by a dedicated PostgreSQL 15 metadata database.

---

## Services

| Service | Image | Port | Role |
|---|---|---|---|
| `postgres` | `postgres:15` | `5432` | Airflow metadata database |
| `airflow-init` | `apache/airflow:2.8.1` | — | One-time DB migration (`db migrate`) |
| `webserver` | `apache/airflow:2.8.1` | `8080` | Airflow UI + admin user bootstrap |
| `scheduler` | `apache/airflow:2.8.1` | — | DAG scheduling & task execution |

---

## Configuration

| Parameter | Value |
|---|---|
| **Executor** | `LocalExecutor` |
| **Metadata DB** | `postgresql+psycopg2://airflow:airflow@postgres/airflow` |
| **Fernet Key** | Set via `AIRFLOW__CORE__FERNET_KEY` |
| **Load Examples** | `False` |
| **Webserver Secret Key** | Set via `AIRFLOW__WEBSERVER__SECRET_KEY` |
| **DAGs Volume** | `./dags` → `/opt/airflow/dags` |
| **Default Admin** | `admin` / `admin` |

---

## Infrastructure Architecture

```mermaid
flowchart TD
    USER["👤 User / Browser"] -- "http://localhost:8080\nadmin / admin" --> WS

    subgraph Docker["🐳 Docker Compose Stack (apache/airflow:2.8.1)"]
        PG["🐘 PostgreSQL 15\nMetadata DB\n:5432"]

        INIT["⚙️ airflow-init\ndb migrate\none-shot"]

        WS["🌐 Airflow Webserver\n:8080\nGunicorn workers"]

        SCH["🕐 Airflow Scheduler\nLocalExecutor"]

        DAG["📄 dags/productr_ecs_feature.py\nDAG: product_recommendation_feature_ingestion\nSchedule: @daily"]

        PG -- "SQL Alchemy conn" --> INIT
        PG -- "SQL Alchemy conn" --> WS
        PG -- "SQL Alchemy conn" --> SCH
        SCH -- "reads & executes" --> DAG
        WS -- "monitors tasks" --> SCH
    end
```

---

## Volume & Network

| Type | Host Path | Container Path | Used By |
|---|---|---|---|
| Bind mount | `./dags` | `/opt/airflow/dags` | `webserver`, `scheduler` |
| Port binding | `localhost:5432` | `5432` | `postgres` |
| Port binding | `localhost:8080` | `8080` | `webserver` |

All services share a single default Docker bridge network (`lamatola-etl_default`).

---

## Health Checks

| Service | Check | Interval | Retries |
|---|---|---|---|
| `postgres` | `pg_isready -U airflow` | 10s | 5 |
| `webserver` | PID file `/opt/airflow/airflow-webserver.pid` exists | 30s | 3 |

---

## Running the Stack

```bash
# Start (first time or after changes)
docker-compose up -d

# View logs
docker-compose logs -f webserver
docker-compose logs -f scheduler

# Stop and remove volumes
docker-compose down --volumes --remove-orphans
```

Access the Airflow UI at **http://localhost:8080** → login with `admin` / `admin`.
