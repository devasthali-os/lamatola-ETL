# product-recs-feature-ingestion

> All documentation is written for **high-throughput, low-latency** systems at Google / Amazon scale —  
> 10B+ events/day · 500M+ entities · < 10 ms online serving SLA

## Docs

| File | What it covers |
|---|---|
| [`product_recs_feature_design.md`](docs/product_recs_feature_design.md) | DAG design, task details, flow diagrams |
| [`001_MLOps.md`](docs/001_MLOps.md) | Local Docker Compose infra, services, health checks, startup order |
| [`002_airflow_best_practices.md`](docs/002_airflow_best_practices.md) | Airflow 2 best practices grounded in this DAG |
| [`003_composer_best_practices.md`](docs/003_composer_best_practices.md) | Google Cloud Composer 2 deployment, CI/CD, scaling |
| [`004_feature_store_best_practices.md`](docs/004_feature_store_best_practices.md) | BigQuery · Vertex AI · Redis · Bigtable — pros, cons, patterns |
| [`005_flyte_feature_etl.md`](docs/005_flyte_feature_etl.md) | Flyte implementation, best practices, comparison with Airflow |
| [`006_step_functions_feature_etl.md`](docs/006_step_functions_feature_etl.md) | AWS Step Functions implementation, best practices, comparison with Airflow |
| [`007_orchestrator_comparison.md`](docs/007_orchestrator_comparison.md) | Airflow vs Flyte vs Step Functions — full three-way comparison + decision guide |
| [`008_pandas_bq_best_practices.md`](docs/008_pandas_bq_best_practices.md) | pandas + BigQuery — feature extraction, transformation, loading, memory management, schema validation |
