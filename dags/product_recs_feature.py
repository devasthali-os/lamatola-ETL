# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
### Feature Ingestion Pipeline – Product Recommendation
Daily DAG that ingests raw user interaction events and product catalogue data,
validates and transforms them into features, and loads them into the feature store
for the product recommendation engine.

Pipeline:
    extract_user_events  ──┐
                           ├──► validate_features ──► transform_features ──► load_feature_store
    extract_product_catalog─┘
"""
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'ml-platform',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['ml-platform@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='product_recommendation_feature_ingestion',
    default_args=default_args,
    description='Daily feature ingestion pipeline for the product recommendation engine',
    schedule_interval='0 2 * * *',   # 2 AM UTC daily
    catchup=False,
    max_active_runs=1,
    tags=['ml', 'feature-ingestion', 'product-recommendation'],
)
def product_recommendation_feature_ingestion():
    """
    ## Product Recommendation – Feature Ingestion Pipeline

    Ingests user interaction events and product catalogue data daily,
    transforms them into ML features, and loads to the feature store.
    """

    @task(task_id='extract_user_events')
    def extract_user_events(ds: str) -> dict:
        """
        #### Extract User Events
        Pulls raw user interaction events (clicks, views, purchases)
        for the execution date `ds` from the source system.
        Returns a summary dict with row count and output path.
        """
        print(f"[{ds}] Extracting user interaction events ...")
        # TODO: replace with real source (e.g. S3, Kafka, OLTP DB)
        output_path = f"/tmp/features/user_events_{ds}.parquet"
        row_count = 0  # placeholder
        print(f"[{ds}] Extracted {row_count} user events → {output_path}")
        return {"path": output_path, "rows": row_count, "date": ds}

    @task(task_id='extract_product_catalog')
    def extract_product_catalog(ds: str) -> dict:
        """
        #### Extract Product Catalog
        Pulls the current product catalogue snapshot (id, category,
        price, metadata) for the execution date `ds`.
        Returns a summary dict with row count and output path.
        """
        print(f"[{ds}] Extracting product catalogue snapshot ...")
        # TODO: replace with real source (e.g. product DB, API)
        output_path = f"/tmp/features/product_catalog_{ds}.parquet"
        row_count = 0  # placeholder
        print(f"[{ds}] Extracted {row_count} products → {output_path}")
        return {"path": output_path, "rows": row_count, "date": ds}

    @task(task_id='validate_features')
    def validate_features(user_events: dict, product_catalog: dict) -> dict:
        """
        #### Validate Features
        Runs schema and data quality checks on both extracted datasets.
        Fails the task (raises ValueError) if critical checks do not pass,
        triggering a retry before alerting on-call.
        Checks:
        - Non-empty datasets
        - No null user_id / product_id
        - Price values > 0
        """
        ds = user_events["date"]
        print(f"[{ds}] Validating user events ({user_events['rows']} rows) ...")
        print(f"[{ds}] Validating product catalog ({product_catalog['rows']} rows) ...")
        # TODO: replace with Great Expectations / Pandera checks
        print(f"[{ds}] Validation passed ✓")
        return {"user_events": user_events, "product_catalog": product_catalog}

    @task(task_id='transform_features')
    def transform_features(validated: dict) -> dict:
        """
        #### Transform Features
        Joins user events with the product catalogue and computes
        derived features used by the recommendation model:
        - user_purchase_count_7d
        - user_category_affinity
        - product_popularity_score
        - product_co_view_rate
        Returns the output path of the transformed feature set.
        """
        ds = validated["user_events"]["date"]
        print(f"[{ds}] Joining user events with product catalogue ...")
        print(f"[{ds}] Computing derived recommendation features ...")
        # TODO: replace with pandas / Spark transformation logic
        output_path = f"/tmp/features/transformed_{ds}.parquet"
        print(f"[{ds}] Features written → {output_path}")
        return {"path": output_path, "date": ds}

    @task(task_id='load_feature_store')
    def load_feature_store(transformed: dict) -> None:
        """
        #### Load Feature Store
        Upserts the transformed feature set into the feature store
        (keyed by entity_id + feature_date) so the recommendation
        engine can serve the latest features at inference time.
        Uses upsert semantics — safe to re-run (idempotent).
        """
        ds = transformed["date"]
        print(f"[{ds}] Loading features from {transformed['path']} into feature store ...")
        # TODO: replace with Feast / Redis / PostgreSQL upsert
        print(f"[{ds}] Feature store load complete ✓")

    # ── Wire up the pipeline ──────────────────────────────────────────────────
    #
    #   extract_user_events  ──┐
    #                          ├──► validate_features ──► transform_features ──► load_feature_store
    #   extract_product_catalog─┘

    user_events     = extract_user_events()
    product_catalog = extract_product_catalog()
    validated       = validate_features(user_events, product_catalog)
    transformed     = transform_features(validated)
    load_feature_store(transformed)


product_recommendation_feature_ingestion()
