# Project 9 — Databricks Retail Lakehouse with Lakeflow and dbt

An end-to-end retail analytics platform built for the current Databricks data-engineering stack. It ingests CDC-style JSON with Auto Loader, applies data-quality rules, preserves rejected data, maintains SCD Type 2 history in Delta Lake, and publishes tested dbt dimensional models.

## What this project demonstrates

- Databricks Free Edition and Unity Catalog Volumes
- Lakeflow Spark Declarative Pipelines using the current `pyspark.pipelines` API
- Bronze, Silver, quarantine, and Gold layers
- Auto Loader incremental ingestion and rescued data
- Expectations for data quality and an inspectable quarantine table
- `AUTO CDC` with SCD Type 2 history
- dbt staging, dimensions, incremental facts, marts, snapshots, and tests
- Databricks Asset Bundles for repeatable deployment and job orchestration
- Local PySpark unit tests and GitHub Actions CI

## Architecture

```text
JSON files in Unity Catalog Volume
              |
              v
 Auto Loader -> bronze_orders
                    |
              quality rules
             /             \
            v               v
 silver_order_changes   quarantined_orders
            |
        AUTO CDC SCD2
            v
 silver_orders_history
            |
       +----+----------------------+
       |                           |
 Lakeflow Gold              dbt semantic layer
 gold_daily_sales      dimensions -> fact -> mart
```

## Repository layout

```text
src/pipelines/retail_pipeline.py  Lakeflow Bronze/Silver/Gold definitions
src/transformations.py            Reusable and locally testable Spark logic
src/setup_sample_data.py          One-time Databricks sample-data notebook
dbt/                              Analytics models, snapshot, and tests
resources/                        Bundle pipeline and workflow resources
databricks.yml                    Bundle targets and variables
tests/                            Local PySpark unit tests
```

## 1. Test locally

From the repository root:

```bash
source venv/bin/activate
python -m pip install -r Project9_Databricks_Delta_Dbt/requirements.txt
SPARK_LOCAL_IP=127.0.0.1 python -m pytest Project9_Databricks_Delta_Dbt/tests -v
```

These tests validate transformation and quality logic without requiring a paid cloud account.

## 2. Prepare Databricks Free Edition

1. Create or sign in to a Databricks Free Edition workspace.
2. Install the Databricks CLI and authenticate with browser-based OAuth:

   ```bash
   databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com --profile project9
   export DATABRICKS_CONFIG_PROFILE=project9
   ```

3. Create the development schema and landing Volume, then upload the included sample data:

   ```bash
   databricks schemas create project9_retail_dev workspace --profile project9
   databricks volumes create workspace project9_retail_dev landing MANAGED --profile project9
   databricks fs mkdir dbfs:/Volumes/workspace/project9_retail_dev/landing/retail_orders --profile project9
   databricks fs cp data/sample_orders.json dbfs:/Volumes/workspace/project9_retail_dev/landing/retail_orders/sample_orders.json --profile project9 --overwrite
   ```

   Alternatively, import and run `src/setup_sample_data.py` once in the workspace.
4. Open **SQL Warehouses**, select a serverless warehouse, and copy its ID from the URL or connection details.

Free Edition has quotas and serverless-only constraints, so this project intentionally does not define a classic cluster.

## 3. Validate and deploy

Replace the warehouse placeholder at deploy time rather than committing a personal ID:

```bash
cd Project9_Databricks_Delta_Dbt
databricks bundle validate -t dev --var="sql_warehouse_id=YOUR_WAREHOUSE_ID"
databricks bundle deploy -t dev --var="sql_warehouse_id=YOUR_WAREHOUSE_ID"
databricks bundle run retail_sales_workflow -t dev --var="sql_warehouse_id=YOUR_WAREHOUSE_ID"
```

The workflow first refreshes the Lakeflow pipeline, then runs `dbt deps` and `dbt build`. A failed dbt test fails the workflow.

## 4. Verify end to end

Use the Databricks SQL editor:

```sql
SELECT * FROM workspace.project9_retail_dev.bronze_orders;
SELECT * FROM workspace.project9_retail_dev.quarantined_orders;
SELECT * FROM workspace.project9_retail_dev.silver_orders_history ORDER BY order_id, __START_AT;
SELECT * FROM workspace.project9_retail_dev.gold_daily_sales;
SELECT * FROM workspace.project9_retail_dev.mart_daily_sales;
```

Expected results:

- Five source events land in Bronze.
- One deliberately invalid event appears in quarantine with a reason.
- The valid update for `O1002` creates SCD Type 2 history.
- Gold and dbt marts contain only current, valid orders.
- The workflow and all dbt tests finish successfully.

The Databricks job runtime creates a dbt profile target named
`databricks_cluster`; the bundle uses that generated target intentionally.

## Production improvements

For a real production workload, add separate catalogs per environment, service-principal authentication, external locations, row/column permissions, monitoring alerts, cost policies, CI deployment approvals, and event-driven file arrival. The Free Edition configuration here stays intentionally small and reproducible.
