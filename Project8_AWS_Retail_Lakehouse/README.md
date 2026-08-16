# Project 8 — Local AWS Retail Lakehouse

A cost-free AWS-style lakehouse running entirely on a developer workstation.
PostgreSQL provides operational retail data, MinIO provides an S3-compatible
object store, and PySpark publishes Apache Iceberg Bronze, Silver, Gold,
quarantine, and control tables.

## Architecture

```text
PostgreSQL retail_sales
  -> Spark JDBC incremental extraction
  -> MinIO bucket (AWS S3-compatible)
       -> Iceberg Bronze: immutable source versions
       -> Iceberg Silver: validated latest order state
       -> Iceberg Gold: daily category metrics
       -> Iceberg control: quarantine + compound watermark
  -> Airflow orchestration
  -> Terraform or Docker Compose local infrastructure
```

This design uses Iceberg `S3FileIO`, which Apache Iceberg recommends for S3
workloads. Iceberg catalog records live in the local PostgreSQL service while
table data and metadata files live in MinIO; a later AWS version can replace
the JDBC catalog with Glue while retaining the table and transformation design.

## Engineering concepts

- S3-compatible object storage and cloud-to-local configuration boundaries
- Iceberg ACID snapshots, `MERGE INTO`, hidden partitioning, and table metadata
- PostgreSQL JDBC extraction with a compound `updated_at`/`order_id` watermark
- Bronze append history, Silver upserts, Gold aggregation, and quarantine
- quality-gated watermark commits and safe no-change runs
- Airflow retries and single-run concurrency
- infrastructure as code using the Docker Terraform provider

## Quick start

Run from the `DataEngineering` repository root:

```bash
cp Project8_AWS_Retail_Lakehouse/.env.example \
   Project8_AWS_Retail_Lakehouse/.env
```

Replace both secret placeholders in `.env`, then install dependencies and
start local infrastructure:

```bash
./venv/bin/python -m pip install -r Project8_AWS_Retail_Lakehouse/requirements.txt
./Project8_AWS_Retail_Lakehouse/scripts/start_local.sh
```

MinIO API is available at <http://localhost:9000>, its console at
<http://localhost:9001>, and PostgreSQL at `localhost:5434`.

Run the pipeline:

```bash
./venv/bin/python Project8_AWS_Retail_Lakehouse/run_pipeline.py
```

The first Spark launch downloads the pinned Iceberg, AWS, and PostgreSQL Maven
packages. Later runs reuse the local Ivy cache. A second run without source
changes returns `status: no_changes` and writes no new snapshot.

Add a source change and run again:

```bash
docker exec -i project8-postgres psql -U retail_user -d retail_lakehouse <<'SQL'
INSERT INTO retail_sales
  (order_id, customer_name, product, category, price, quantity, city, order_date)
VALUES
  (5, 'Meera', 'Headphones', 'Electronics', 3000, 2, 'Mumbai', CURRENT_DATE);
SQL

./venv/bin/python Project8_AWS_Retail_Lakehouse/run_pipeline.py
```

## Automated tests

```bash
./venv/bin/python -m pytest Project8_AWS_Retail_Lakehouse/tests -v
```

The unit test verifies cleaning, latest-version deduplication, derived revenue,
and daily/category aggregation without requiring MinIO or PostgreSQL.

## Airflow

Point a local Airflow installation at `dags/retail_lakehouse_pipeline.py`. The
DAG first verifies PostgreSQL and MinIO, then runs the quality-gated pipeline.
It has two retries, manual scheduling, and `max_active_runs=1`.

## Terraform alternative

`terraform/main.tf` expresses the local PostgreSQL and MinIO infrastructure
through `kreuzwerker/docker`. Docker Compose is the quickest development path;
Terraform demonstrates state-driven provisioning and secret variables.

```bash
cd Project8_AWS_Retail_Lakehouse/terraform
terraform init
terraform plan \
  -var='postgres_password=<local-password>' \
  -var='minio_secret_key=<local-secret>'
```

Do not run Compose and Terraform simultaneously because both intentionally use
the same container names and host ports.

## AWS migration path

For real AWS deployment, replace MinIO with S3, the JDBC catalog with AWS
Glue, local Spark with EMR/Glue, local credentials with IAM roles, and the
Docker Terraform resources with AWS provider resources. Keep S3FileIO,
Iceberg schemas, transformations, quality rules, and orchestration boundaries.

## Cleanup

```bash
docker compose --env-file Project8_AWS_Retail_Lakehouse/.env \
  -f Project8_AWS_Retail_Lakehouse/docker-compose.yml down
```

Add `--volumes` only when intentionally deleting the local database and all
Iceberg objects.

## Verified local result

The implementation was verified on August 16, 2026 with an initial four-row
load, a zero-row `no_changes` run, a one-row incremental insert, and an update
to that order. The Iceberg `MERGE INTO` retained five unique Silver orders,
Gold retained five daily/category groups, and both layers reconciled to revenue
of `160000.00`. Project 8's PySpark unit test, Airflow DAG import, Python
compilation, shell syntax, Compose validation, and repository formatting checks
pass.
