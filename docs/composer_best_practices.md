# Google Cloud Composer Best Practices

> Applies to: `product_recommendation_feature_ingestion` DAG  
> File: `dags/product_recs_feature.py`  
> Target platform: Google Cloud Composer 2 (Airflow 2.8.x · GKE Autopilot)  
> Current local stack: Airflow 2.8.1 · Docker Compose (dev)  
> Last updated: 2026-03-29

---

## Table of Contents

1. [Composer Environment Setup](#1-composer-environment-setup)
2. [DAG Deployment](#2-dag-deployment)
3. [DAG & Task Design for Composer](#3-dag--task-design-for-composer)
4. [GCP-Native Operators](#4-gcp-native-operators)
5. [Secrets & Configuration](#5-secrets--configuration)
6. [Networking & IAM](#6-networking--iam)
7. [Scaling & Performance](#7-scaling--performance)
8. [Logging, Monitoring & Alerting](#8-logging-monitoring--alerting)
9. [CI/CD Pipeline](#9-cicd-pipeline)
10. [Cost Optimisation](#10-cost-optimisation)
11. [Local → Composer Migration Checklist](#11-local--composer-migration-checklist)

---

## 1. Composer Environment Setup

### ✅ Use Composer 2 (Autopilot GKE) over Composer 1
Composer 2 runs on GKE Autopilot — nodes scale automatically, no manual node pool management.

```bash
gcloud composer environments create prod-ml-feature-ingestion \
  --location us-central1 \
  --image-version composer-2.6.6-airflow-2.8.1 \
  --environment-size small \
  --service-account composer-sa@PROJECT_ID.iam.gserviceaccount.com
```

### ✅ Match Airflow version to local dev
Keep `composer-2.x.x-airflow-2.8.1` in sync with the local `apache/airflow:2.8.1` Docker image.
This eliminates "works locally, breaks in prod" issues.

| Environment | Airflow version | Python |
|---|---|---|
| Local Docker Compose (dev) | 2.8.1 | 3.8 |
| Composer 2 (prod) | 2.8.1 | 3.8 |

### ✅ Use separate environments per stage

| Environment | Purpose | Size |
|---|---|---|
| `dev-ml-feature-ingestion` | Developer testing | Small |
| `staging-ml-feature-ingestion` | Pre-production validation | Small |
| `prod-ml-feature-ingestion` | Production daily runs | Medium |

### ✅ Pin the Composer image version in IaC (Terraform)

```hcl
resource "google_composer_environment" "prod" {
  name   = "prod-ml-feature-ingestion"
  region = "us-central1"

  config {
    software_config {
      image_version = "composer-2.6.6-airflow-2.8.1"

      pypi_packages = {
        "pandas"            = "==2.2.0"
        "pyarrow"           = "==15.0.0"
        "great-expectations" = "==0.18.0"
      }

      airflow_config_overrides = {
        "core-max_active_runs_per_dag" = "1"
        "core-load_examples"           = "False"
        "scheduler-catchup_by_default" = "False"
      }
    }
  }
}
```

---

## 2. DAG Deployment

### ✅ Sync DAGs via the Composer GCS bucket

Every Composer environment has a dedicated GCS bucket. Anything placed in the `dags/`
folder is automatically picked up by the scheduler within ~3 minutes.

```bash
# One-off manual deploy
gcloud composer environments storage dags import \
  --environment prod-ml-feature-ingestion \
  --location us-central1 \
  --source dags/product_recs_feature.py

# Or sync the whole dags/ folder
gsutil -m rsync -r dags/ \
  gs://$(gcloud composer environments describe prod-ml-feature-ingestion \
    --location us-central1 \
    --format="value(config.dagGcsPrefix)")/
```

### ✅ Never edit DAGs directly in the GCS bucket
Always go through version control → CI/CD → `gsutil rsync`. Direct bucket edits are
not reproducible and break rollback.

### ✅ Trigger a DAG parse check before deploying

```bash
# Run inside a local Airflow container to validate before pushing to Composer
python dags/product_recs_feature.py
```

### ✅ Deploy Python dependencies via `pypi_packages` — not `requirements.txt` mounted at runtime

```bash
# Add a package to the running Composer environment
gcloud composer environments update prod-ml-feature-ingestion \
  --location us-central1 \
  --update-pypi-package pandas==2.2.0
```

---

## 3. DAG & Task Design for Composer

### ✅ The current `@dag` + `@task` TaskFlow API is fully supported

```python
# ✅ Works identically in Composer 2 — no changes needed to the DAG structure
from airflow.decorators import dag, task

@dag(
    dag_id='product_recommendation_feature_ingestion',
    schedule_interval='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['ml', 'feature-ingestion', 'product-recommendation'],
)
def product_recommendation_feature_ingestion():
    ...
```

### ✅ Replace `/tmp/features/` paths with GCS paths
Local task file I/O (`/tmp/features/*.parquet`) does not work across GKE pods in Composer.
Each task runs in its own pod — there is no shared local filesystem.

```python
# ❌ Current — breaks in Composer (each task runs in a different pod)
output_path = f"/tmp/features/user_events_{ds}.parquet"

# ✅ For Composer — use GCS as the shared intermediate store
GCS_BUCKET = "gs://ml-feature-ingestion-bucket"

@task
def extract_user_events(ds: str) -> dict:
    output_path = f"{GCS_BUCKET}/raw/user_events_{ds}.parquet"
    # df.to_parquet(output_path)  ← works with pandas + gcsfs
    return {"path": output_path, "rows": row_count, "date": ds}
```

```bash
# Install GCS filesystem support
gcloud composer environments update prod-ml-feature-ingestion \
  --location us-central1 \
  --update-pypi-package gcsfs==2024.2.0
```

### ✅ Keep XCom payloads small (already applied)
XCom is stored in Cloud SQL (Composer's Airflow metadata DB). The current pattern of
passing a `dict` with `path` + `rows` + `date` is correct and safe.

### ✅ `catchup=False` and `max_active_runs=1` (already applied)
Critical in Composer — concurrent runs waste GKE pods and can cause feature store write conflicts.

### ✅ Set `execution_timeout` per task to prevent runaway pods

```python
@task(execution_timeout=timedelta(hours=1))
def transform_features(validated: dict) -> dict:
    ...
```

---

## 4. GCP-Native Operators

### ✅ Replace placeholder logic with GCP-native operators where possible

| Current placeholder | Recommended Composer operator | Use case |
|---|---|---|
| `extract_user_events` print stub | `BigQueryToGCSOperator` | Export raw events from BQ to GCS |
| `extract_product_catalog` print stub | `BigQueryOperator` | Query product catalogue from BQ |
| `validate_features` print stub | `BigQueryCheckOperator` | Data quality SQL checks |
| `transform_features` print stub | `DataprocSubmitJobOperator` | PySpark feature transforms |
| `load_feature_store` print stub | `BigQueryInsertJobOperator` | Upsert features into BQ feature table |

### Example: swap `extract_user_events` for a BigQuery export

```python
from airflow.providers.google.cloud.operators.bigquery import BigQueryToGCSOperator

@dag(...)
def product_recommendation_feature_ingestion():

    extract_user_events = BigQueryToGCSOperator(
        task_id='extract_user_events',
        source_project_dataset_table='my_project.events.user_interactions',
        destination_cloud_storage_uris=[
            'gs://ml-feature-ingestion-bucket/raw/user_events_{{ ds }}.parquet'
        ],
        export_format='PARQUET',
        gcp_conn_id='google_cloud_default',
    )
```

### ✅ Use `gcp_conn_id='google_cloud_default'`
In Composer, `google_cloud_default` is pre-configured with the Composer service account.
No credentials needed in code.

---

## 5. Secrets & Configuration

### ❌ Never use hardcoded credentials (already flagged in `airflow_best_practices.md`)

### ✅ Use Google Secret Manager as the Airflow secrets backend

```bash
# Enable Secret Manager backend in Composer
gcloud composer environments update prod-ml-feature-ingestion \
  --location us-central1 \
  --update-airflow-configs \
    secrets-backend=airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend \
    secrets-backend_kwargs='{"project_id":"MY_PROJECT","connections_prefix":"airflow-connections","variables_prefix":"airflow-variables"}'
```

```bash
# Store a connection secret
echo -n "postgresql+psycopg2://user:pass@host/features" | \
  gcloud secrets create airflow-connections-feature_store_db \
    --data-file=- \
    --project MY_PROJECT
```

```python
# ✅ No change needed in the DAG — Airflow resolves it transparently
from airflow.providers.postgres.hooks.postgres import PostgresHook
hook = PostgresHook(postgres_conn_id='feature_store_db')
```

### ✅ Use Airflow Variables backed by Secret Manager for pipeline params

```bash
gcloud secrets create airflow-variables-feature_window_days \
  --data-file=<(echo -n "7") \
  --project MY_PROJECT
```

```python
from airflow.models import Variable
FEATURE_WINDOW_DAYS = int(Variable.get('feature_window_days', default_var=7))
```

### ✅ Use Workload Identity — no service account key files

```bash
# Bind the Composer SA to a GCP SA with Workload Identity
gcloud iam service-accounts add-iam-policy-binding \
  composer-sa@MY_PROJECT.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:MY_PROJECT.svc.id.goog[composer-1/composer-sa]"
```

---

## 6. Networking & IAM

### ✅ Run Composer in a private IP environment (no public IP on GKE nodes)

```bash
gcloud composer environments create prod-ml-feature-ingestion \
  --location us-central1 \
  --enable-private-environment \
  --network my-vpc \
  --subnetwork my-subnet
```

### ✅ Minimal IAM — grant only what Composer needs

| Principal | Role | Why |
|---|---|---|
| Composer SA | `roles/bigquery.dataEditor` | Read/write BQ feature tables |
| Composer SA | `roles/storage.objectAdmin` on feature bucket | Read/write GCS parquet files |
| Composer SA | `roles/secretmanager.secretAccessor` | Read Secret Manager secrets |
| Composer SA | `roles/dataproc.editor` | Submit Dataproc jobs (transform) |
| CI/CD SA | `roles/composer.worker` | Deploy DAG files to GCS bucket |

```bash
gcloud projects add-iam-policy-binding MY_PROJECT \
  --member="serviceAccount:composer-sa@MY_PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"
```

### ✅ Use VPC Service Controls to restrict data exfiltration
Wrap BigQuery, GCS, and Secret Manager in a VPC-SC perimeter so feature data cannot
leave the project boundary.

---

## 7. Scaling & Performance

### ✅ Use KubernetesPodOperator for heavy transforms
For the `transform_features` task (pandas / Spark joins on large datasets), run it in
a dedicated pod with custom CPU/memory instead of the shared Airflow worker.

```python
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

transform_features = KubernetesPodOperator(
    task_id='transform_features',
    name='feature-transform',
    namespace='composer-1',
    image='gcr.io/MY_PROJECT/feature-transform:1.0.0',
    cmds=['python', 'transform.py'],
    arguments=['--date', '{{ ds }}'],
    container_resources={
        'request_cpu': '2',
        'request_memory': '4Gi',
        'limit_cpu': '4',
        'limit_memory': '8Gi',
    },
    get_logs=True,
    is_delete_operator_pod=True,
)
```

### ✅ Set Airflow config overrides for the feature ingestion workload

```bash
gcloud composer environments update prod-ml-feature-ingestion \
  --location us-central1 \
  --update-airflow-configs \
    core-parallelism=32 \
    core-max_active_tasks_per_dag=5 \
    scheduler-max_dagruns_to_create_per_loop=5
```

### ✅ Environment sizing guide for this pipeline

| Stage | `extract` tasks | `validate` | `transform` | `load` | Recommended size |
|---|---|---|---|---|---|
| Dev / staging | < 10K rows | fast | < 1 min | fast | Small (2 schedulers) |
| Production | millions of events | seconds | 5–30 min | minutes | Medium + KPO for transform |

---

## 8. Logging, Monitoring & Alerting

### ✅ Airflow task logs go to Cloud Logging automatically
In Composer, no extra configuration needed — every `print()` and `logging` call from
`@task` functions appears in Cloud Logging under the `airflow-worker` log name.

```bash
# Query logs for a specific DAG run from gcloud
gcloud logging read \
  'resource.type="cloud_composer_environment" AND
   jsonPayload.dag_id="product_recommendation_feature_ingestion" AND
   severity>=ERROR' \
  --limit 50 \
  --project MY_PROJECT
```

### ✅ Create a Cloud Monitoring alerting policy for task failures

```bash
# Alert when any task in the feature ingestion DAG fails
gcloud alpha monitoring policies create \
  --policy-from-file monitoring/feature_ingestion_alert_policy.json
```

```json
// monitoring/feature_ingestion_alert_policy.json
{
  "displayName": "Feature Ingestion Task Failure",
  "conditions": [{
    "displayName": "Airflow task failure",
    "conditionThreshold": {
      "filter": "metric.type=\"composer.googleapis.com/environment/dag/task/failed_runs\" resource.type=\"cloud_composer_environment\"",
      "comparison": "COMPARISON_GT",
      "thresholdValue": 0,
      "duration": "0s"
    }
  }],
  "notificationChannels": ["projects/MY_PROJECT/notificationChannels/SLACK_CHANNEL_ID"],
  "alertStrategy": { "autoClose": "604800s" }
}
```

### ✅ Use Cloud Composer's built-in DAG metrics dashboard
Navigate to: **Composer environment → Monitoring tab → DAG runs / Task duration / Scheduler heartbeat**

### ✅ Add Airflow SLA misses for late feature delivery

```python
# Features must land in the feature store within 2 hours of the 2 AM trigger
@dag(
    ...
    sla_miss_callback=sla_miss_alert,
    default_args={
        ...
        'sla': timedelta(hours=2),   # alert if features not ready by 04:00 UTC
    }
)
```

---

## 9. CI/CD Pipeline

### ✅ Full GitOps deployment to Composer via GitHub Actions

```yaml
# .github/workflows/deploy_dags.yml
name: Deploy DAGs to Composer

on:
  push:
    branches: [main]
    paths: [dags/**]

jobs:
  lint-and-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with: { python-version: '3.8' }

      - name: Install dependencies
        run: pip install apache-airflow==2.8.1 pytest ruff

      - name: Lint
        run: ruff check dags/

      - name: Parse DAG
        run: python dags/product_recs_feature.py

      - name: Run DAG structure tests
        run: pytest tests/test_product_recs_dag.py -v

  deploy:
    needs: lint-and-test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Authenticate to GCP
        uses: google-github-actions/auth@v2
        with:
          credentials_json: ${{ secrets.GCP_COMPOSER_SA_KEY }}

      - name: Deploy DAGs to Composer GCS bucket
        uses: google-github-actions/upload-cloud-storage@v2
        with:
          path: dags/
          destination: ${{ secrets.COMPOSER_DAG_BUCKET }}/dags
```

### ✅ Deployment flow

```mermaid
flowchart LR
    DEV["👨‍💻 Dev\nPush to branch"] --> PR["Pull Request\nCode review"]
    PR --> CI

    subgraph CI["GitHub Actions – CI"]
        LINT["ruff lint"]
        PARSE["python dags/product_recs_feature.py"]
        TEST["pytest DAG structure\n+ task unit tests"]
        LINT --> PARSE --> TEST
    end

    CI -- pass --> MERGE["Merge to main"]
    MERGE --> CD

    subgraph CD["GitHub Actions – CD"]
        AUTH["Authenticate\nWorkload Identity"]
        SYNC["gsutil rsync\ndags/ → GCS bucket"]
        AUTH --> SYNC
    end

    SYNC --> COMPOSER["☁️ Composer Scheduler\nauto-picks new DAG\nwithin ~3 min"]
```

### ✅ Roll back a bad DAG deployment

```bash
# Restore the previous version from git and redeploy
git checkout HEAD~1 -- dags/product_recs_feature.py
gsutil cp dags/product_recs_feature.py \
  gs://COMPOSER_BUCKET/dags/product_recs_feature.py
```

---

## 10. Cost Optimisation

### ✅ Use `environment_size=small` for dev/staging

```bash
# Downsize non-prod environments to save cost
gcloud composer environments update dev-ml-feature-ingestion \
  --location us-central1 \
  --environment-size small
```

### ✅ Pause the DAG in non-prod environments when not in use

```bash
# Pause via CLI to stop scheduled runs (environment still running)
gcloud composer environments run dev-ml-feature-ingestion \
  --location us-central1 \
  dags pause -- product_recommendation_feature_ingestion
```

### ✅ Use Spot / preemptible nodes for KubernetesPodOperator tasks

```python
transform_features = KubernetesPodOperator(
    ...
    tolerations=[{
        'key': 'cloud.google.com/gke-spot',
        'operator': 'Equal',
        'value': 'true',
        'effect': 'NoSchedule',
    }],
    node_selector={'cloud.google.com/gke-spot': 'true'},
)
```

### ✅ Schedule around BigQuery free tier quotas
BigQuery charges per byte scanned. Feature extraction queries should:
- Target partitioned tables using `WHERE date = '{{ ds }}'`
- Use `LIMIT` in dev/staging
- Enable BigQuery slot reservations for consistent cost in prod

### ✅ Set task timeouts to avoid runaway billing

```python
@task(execution_timeout=timedelta(hours=1))
def transform_features(validated: dict) -> dict:
    ...   # killed after 1h → prevents unbounded Dataproc / BQ charges
```

---

## 11. Local → Composer Migration Checklist

Track the work needed to move from the current Docker Compose dev setup to Cloud Composer production.

```mermaid
flowchart TD
    LOCAL["🐳 Local Docker Compose\napache/airflow:2.8.1\nLocalExecutor\nPostgreSQL 15"] -->|migrate| COMPOSER["☁️ Cloud Composer 2\ncomposer-2.6.6-airflow-2.8.1\nKubernetes Executor\nCloud SQL"]
```

| # | Item | Status | Notes |
|---|---|---|---|
| 1 | Airflow version parity (`2.8.1`) | ✅ Ready | Local and Composer match |
| 2 | `@dag` + `@task` TaskFlow API | ✅ Ready | Fully supported in Composer 2 |
| 3 | `catchup=False`, `max_active_runs=1` | ✅ Ready | Already set |
| 4 | DAG tags | ✅ Ready | Already set |
| 5 | Replace `/tmp/features/` with GCS paths | ❌ Todo | Critical — shared filesystem does not exist in Composer |
| 6 | Move secrets to Secret Manager | ❌ Todo | Replace hardcoded `docker-compose.yml` values |
| 7 | Register Airflow Connections in Composer | ❌ Todo | `feature_store_db`, source system connections |
| 8 | Replace placeholder `print` stubs with real logic | ❌ Todo | Implement BigQuery / GCS operators |
| 9 | Add `execution_timeout` to heavy tasks | ⚠️ Todo | Especially `transform_features` |
| 10 | Add `KubernetesPodOperator` for transform | ⚠️ Todo | For large-scale feature computation |
| 11 | Set up Cloud Monitoring alert policy | ❌ Todo | Task failure + SLA miss alerts |
| 12 | Set up GitHub Actions CI/CD to Composer GCS | ❌ Todo | Replace manual `gsutil cp` |
| 13 | IAM — Composer SA with minimal permissions | ❌ Todo | BQ, GCS, Secret Manager, Dataproc |
| 14 | Private IP Composer environment | ⚠️ Todo | Required for production security posture |
| 15 | Terraform IaC for Composer environment | ⚠️ Todo | Reproducible environment provisioning |

