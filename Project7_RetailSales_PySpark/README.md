# Project 7 — Retail Sales PySpark Pipeline

Project 7 is a production-inspired learning pipeline for incremental PySpark
processing with PostgreSQL, Parquet Bronze/Silver/Gold layers, data quality,
analytics outputs, and Airflow orchestration.

It demonstrates how to build a restartable, observable batch pipeline rather
than a one-off Spark script. Project 6's PostgreSQL source is reused, while the
processing, lake storage, analytics publication, and orchestration are owned by
Project 7.

## Architecture and data flow

```text
PostgreSQL public.retail_sales
  -> incremental Spark JDBC query (updated_at, order_id watermark)
  -> Bronze: immutable, source-shaped Parquet batches
  -> Silver: cleaned, validated, deduplicated current-state Parquet
       + rejected records -> immutable quarantine Parquet
  -> Gold: six business aggregation snapshots
  -> PostgreSQL analytics_* serving tables
  -> quality gate -> atomic watermark commit

Apache Airflow orchestrates the stages and skips processing when extraction
finds no source changes.
```

### Why these technologies

- **PySpark 3.5.5** provides DataFrame transformations, distributed execution,
  Parquet support, JDBC integration, caching, window functions, and AQE.
- **PostgreSQL** acts as both the operational source and the reporting-serving
  database, making the movement between OLTP-shaped and analytical data clear.
- **Parquet** supplies typed, compressed, columnar lake storage with predicate
  and partition pruning.
- **Apache Airflow 2.11.0** supplies dependency management, retries, branching,
  run history, task logs, and manual scheduling.
- **pytest** provides deterministic transformation tests using small in-memory
  Spark DataFrames.

## Repository layout

```text
Project7_RetailSales_PySpark/
├── config/                 # validated environment configuration
├── dags/                   # Airflow DAG
├── data/
│   ├── bronze/             # immutable incremental source batches
│   ├── silver/             # trusted current-state snapshot
│   ├── gold/               # analytical snapshots
│   ├── quarantine/         # rejected rows and reasons
│   └── state/              # committed compound watermark
├── jars/                   # local PostgreSQL JDBC driver (ignored by Git)
├── logs/                   # pipeline and quality reports (ignored by Git)
├── scripts/                # environment, source, and Airflow helpers
├── spark/                  # extraction through quality/commit modules
├── sql/                    # idempotent source migration and analytics DDL
├── tests/                  # local PySpark unit tests
├── .env.example            # secret-free configuration template
├── requirements.txt        # pinned Python dependencies
└── run_pipeline.py         # stage-oriented CLI
```

## Current status

Steps 1 through 14 provide the Git-friendly scaffold, environment-backed
configuration, source change tracking, PostgreSQL/JDBC verification,
incremental Bronze extraction, a cleaned current-state Silver snapshot, and
business-ready Gold aggregations, PostgreSQL analytics serving tables, and
quality-gated incremental watermark state, explicit validation, and retry-safe
invalid-record quarantine, Airflow orchestration, verified end-to-end DAG runs
for both changed and unchanged sources, passing PySpark unit tests,
runtime-aware Spark optimizations, complete operating documentation, and final
Git/security/runtime verification.

## Prerequisites and first-time setup

The verified local environment uses Linux, Python 3.12, Java 21, Docker,
PostgreSQL, and the repository-level `venv`. Run commands below from the
`DataEngineering` repository root.

1. Create or activate the repository virtual environment and install the
   pinned dependencies:

   ```bash
   python3 -m venv venv
   ./venv/bin/python -m pip install -r Project7_RetailSales_PySpark/requirements.txt
   ```

2. Create the local configuration and set the real database password:

   ```bash
   cp Project7_RetailSales_PySpark/.env.example \
      Project7_RetailSales_PySpark/.env
   ```

3. Put PostgreSQL JDBC driver `postgresql-42.7.7.jar` in
   `Project7_RetailSales_PySpark/jars/`. The binary is intentionally ignored by
   Git.

4. Start the existing Project 6 PostgreSQL container and validate the local
   environment:

   ```bash
   docker start retail_postgres
   ./Project7_RetailSales_PySpark/scripts/check_environment.sh
   ```

5. Apply the source migration and create the analytics tables:

   ```bash
   docker exec -i retail_postgres psql -U retail_user -d retail_db \
     < Project7_RetailSales_PySpark/sql/source_migration.sql
   docker exec -i retail_postgres psql -U retail_user -d retail_db \
     < Project7_RetailSales_PySpark/sql/analytics_tables.sql
   ./venv/bin/python Project7_RetailSales_PySpark/scripts/check_source.py
   ```

The `.env`, JDBC binary, lake contents, local Airflow metadata, and runtime
logs are ignored so credentials and generated artifacts are not committed.

## Configuration check

From the repository root:

```bash
./venv/bin/python Project7_RetailSales_PySpark/run_pipeline.py --check-config
```

Copy `.env.example` to `.env` and replace its password placeholder before the
PostgreSQL connectivity work in Step 2. The `.env` file is ignored by Git.

## Source connectivity check

Project 7 adds an operational `updated_at` column and trigger to the existing
`retail_sales` table. This supports changed-record extraction while remaining
compatible with Project 6. It intentionally does not invent a business
`order_date`.

Apply the idempotent migration:

```bash
docker exec -i retail_postgres psql -U retail_user -d retail_db \
  < Project7_RetailSales_PySpark/sql/source_migration.sql
```

Then test both psycopg2 and Spark JDBC connectivity:

```bash
./venv/bin/python Project7_RetailSales_PySpark/scripts/check_source.py
```

## Bronze extraction

Run from the repository root:

```bash
./venv/bin/python Project7_RetailSales_PySpark/run_pipeline.py --stage extract
```

On the first run, the source is fully extracted. After a successful pipeline
eventually commits its compound `updated_at`/`order_id` watermark, later runs
query only newer or changed records. Step 3 deliberately reads but does not
advance the watermark; watermark publication is quality-gated in Step 7.

Bronze data is source-shaped Parquet with `ingested_at`, `ingestion_date`, and
`batch_id` metadata. It is stored under:

```text
data/bronze/retail_sales/ingestion_date=YYYY-MM-DD/batch_id=<batch-id>/
```

## Silver transformation

Transform one immutable Bronze batch using its batch ID:

```bash
./venv/bin/python Project7_RetailSales_PySpark/run_pipeline.py \
  --stage transform \
  --batch-id step3_initial_20260814
```

The transformation trims text, enforces types, validates required values,
calculates `total_amount`, keeps the newest version of each `order_id`, and
publishes `data/silver/retail_sales/` as the trusted current-state snapshot.
Invalid rows are preserved with rejection reasons under `data/quarantine/`.

## Gold aggregations

Build all Gold datasets from the current Silver snapshot:

```bash
./venv/bin/python Project7_RetailSales_PySpark/run_pipeline.py --stage aggregate
```

Outputs are published under `data/gold/` for overall, category, city, product,
top-product, and customer sales. Each dimensional revenue total is reconciled
to Silver before any Gold snapshot is published. Daily sales are intentionally
excluded because the source has no genuine business `order_date`.

## PostgreSQL analytics load

Load every Gold snapshot through a unique staging table and publish all target
tables in one PostgreSQL transaction:

```bash
./venv/bin/python Project7_RetailSales_PySpark/run_pipeline.py --stage load
```

Only business-ready Gold results are loaded into PostgreSQL. Bronze and Silver
remain in Parquet because they are processing/history layers rather than
low-latency reporting tables.

## Incremental watermark

After Bronze, Silver, Gold, and PostgreSQL succeed for a non-empty batch,
publish its compound watermark:

```bash
./venv/bin/python Project7_RetailSales_PySpark/run_pipeline.py \
  --stage commit \
  --batch-id <successful-batch-id>
```

The commit checks Bronze-to-Silver version lineage, Silver uniqueness, Gold to
PostgreSQL row counts, and Silver-to-PostgreSQL revenue before atomically
writing `data/state/retail_sales_watermark.json`. A failed or empty run cannot
advance the state.

## Data quality and quarantine

Run cross-layer validation for a processed batch:

```bash
./venv/bin/python Project7_RetailSales_PySpark/run_pipeline.py \
  --stage quality \
  --batch-id <batch-id>
```

Invalid records retain their source values plus `rejection_reasons` and a
stable fingerprint under `data/quarantine/retail_sales/`. Quality checks
reconcile Bronze into valid plus quarantined rows, verify Silver rules and
lineage, reconcile every Gold revenue total, and compare Gold with PostgreSQL.
Reports are written as ignored runtime JSON files under `logs/`.

### Manual end-to-end run

When the source contains an unprocessed insert or update, choose one unique
batch ID and use it consistently:

```bash
./venv/bin/python Project7_RetailSales_PySpark/run_pipeline.py \
  --stage extract --batch-id manual_20260816_01
./venv/bin/python Project7_RetailSales_PySpark/run_pipeline.py \
  --stage transform --batch-id manual_20260816_01
./venv/bin/python Project7_RetailSales_PySpark/run_pipeline.py --stage aggregate
./venv/bin/python Project7_RetailSales_PySpark/run_pipeline.py --stage load
./venv/bin/python Project7_RetailSales_PySpark/run_pipeline.py \
  --stage quality --batch-id manual_20260816_01
./venv/bin/python Project7_RetailSales_PySpark/run_pipeline.py \
  --stage commit --batch-id manual_20260816_01
```

Stop if any command fails. The commit must remain last. If extraction reports
zero rows, there is no Bronze batch to transform; that is a successful
no-change result and is handled automatically by the Airflow branch.

## Airflow orchestration

Start the Project 7 Airflow environment from the repository root:

```bash
./Project7_RetailSales_PySpark/scripts/run_airflow.sh
```

Open <http://localhost:8080>, enable `retail_sales_pyspark_pipeline`, and
trigger it manually. The DAG runs:

```text
start
  -> generate_batch_id
  -> extract_to_bronze
  -> check_for_source_changes
       -> no_changes -> end
       -> transform_to_silver
          -> create_gold
          -> load_gold_to_postgres
          -> run_data_quality_checks
          -> commit_watermark
          -> end
```

The batch ID is stable across task retries. `max_active_runs=1` prevents two
runs from publishing local snapshots or watermark state concurrently. An
unchanged source follows the short `no_changes` branch and does not create an
empty Bronze batch or run unnecessary downstream Spark jobs.

### End-to-end DAG test

With Project 7's Airflow environment variables configured by
`scripts/run_airflow.sh`, the equivalent CLI verification is:

```bash
AIRFLOW_HOME="$PWD/Project7_RetailSales_PySpark/airflow" \
AIRFLOW__CORE__DAGS_FOLDER="$PWD/Project7_RetailSales_PySpark/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
./venv/bin/airflow dags test retail_sales_pyspark_pipeline <logical-date>
```

Step 10 verified two successful runs: an unchanged source selected
`no_changes`, while a new valid order executed every processing task and
committed its watermark after all ten quality checks passed.

## Automated tests

Run the Step 11 unit tests from the repository root:

```bash
./venv/bin/python -m pytest Project7_RetailSales_PySpark/tests -v
```

The tests use a lightweight local Spark session and small in-memory records.
They verify field cleaning and classification, `total_amount`, latest-version
deduplication, and invalid-row rejection reasons without changing PostgreSQL or
the project's Bronze, Silver, Gold, quarantine, or watermark data.

## Spark performance optimization

Step 12 makes the execution behavior explicit in the shared Spark session:

- Adaptive Query Execution (AQE) coalesces small post-shuffle partitions at
  runtime, while `SPARK_SHUFFLE_PARTITIONS` remains the configurable starting
  point for larger workloads.
- AQE skew handling can split unusually large join partitions instead of
  allowing one slow task to delay the whole stage.
- dynamic partition pruning and Parquet filter pushdown reduce unnecessary
  reads. The Bronze `batch_id` filter can therefore prune old batch partitions.
- the quality stage now calculates each Gold dataset's row count and revenue in
  one aggregation. This removes four repeated Parquet reads and four separate
  Spark actions from a changed-data quality run.

The existing selective caching remains intentional: Bronze, Silver, and Gold
frames that feed multiple actions use `MEMORY_AND_DISK` and are always
unpersisted. Small published snapshots still use one output file to avoid a
small-file problem in this learning dataset; that choice should be revisited
when individual datasets become large.

Inspect the active optimization settings manually:

```bash
./venv/bin/python - <<'PY'
import sys
sys.path.insert(0, "Project7_RetailSales_PySpark")
from config import Settings
from spark.session import create_spark_session

spark = create_spark_session(Settings.from_env(), "Project7ExplainOptimization")
for key in (
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.optimizer.dynamicPartitionPruning.enabled",
    "spark.sql.parquet.filterPushdown",
):
    print(f"{key}={spark.conf.get(key)}")
spark.stop()
PY
```

## Data contracts and outputs

| Layer | Grain | Important fields | Behavior |
|---|---|---|---|
| Source | One source order version | `order_id`, product/customer fields, `updated_at` | PostgreSQL trigger maintains change timestamp |
| Bronze | One extracted source version | Source fields plus `ingested_at`, `ingestion_date`, `batch_id` | Append-only and partitioned by ingestion date and batch |
| Silver | One latest valid row per `order_id` | Clean source fields, `total_amount`, lineage metadata | Complete current-state snapshot |
| Quarantine | One rejected Bronze record | Cleaned fields, `rejection_reasons`, fingerprint | Immutable per batch and retry-safe |
| Gold | One row per aggregation key | Revenue, orders, units, average order value | Complete analytical snapshots |
| Watermark | One committed pipeline position | UTC `updated_at`, tie-breaking `order_id`, batch ID | Written atomically only after quality succeeds |

Gold produces `overall_sales`, `category_sales`, `city_sales`,
`product_sales`, `top_products`, and `customer_sales`. PostgreSQL exposes the
corresponding `analytics_*` tables. No daily aggregation is fabricated because
the source has no genuine business event date.

## Reliability and idempotency

- The compound watermark prevents missing rows that share the same
  `updated_at`; `order_id` provides deterministic tie-breaking.
- Bronze batches and quarantine batches are immutable. Reusing a conflicting
  batch ID fails instead of silently overwriting history.
- Silver and Gold publish through temporary directories and backups, so a
  partially written snapshot is not exposed as the current version.
- Gold is staged into uniquely named PostgreSQL tables and all serving tables
  are replaced in one transaction under an advisory lock.
- The watermark is the final commit. Failed transformation, publication,
  loading, or validation therefore leaves the source changes eligible for a
  later retry.
- Airflow uses a stable batch ID across retries and permits only one active DAG
  run, which protects the local snapshot and state-file publication model.

## Logs and monitoring

- Pipeline log: `Project7_RetailSales_PySpark/logs/pipeline.log`
- Quality reports: `Project7_RetailSales_PySpark/logs/quality_<batch-id>.json`
- Airflow task logs: open the DAG run, select a task such as
  `run_data_quality_checks`, then select **Log**.
- Local Airflow files: `Project7_RetailSales_PySpark/airflow/logs/`

Useful terminal commands:

```bash
tail -f Project7_RetailSales_PySpark/logs/pipeline.log
find Project7_RetailSales_PySpark/airflow/logs -type f -name '*.log' | sort
```

The expected local Spark messages about the native Hadoop library and hostname
selection are warnings, not failed quality checks. Investigate Python
tracebacks, Airflow task state, and the final `QualityResult` instead.

## Troubleshooting and recovery

- **`JAVA_GATEWAY_EXITED`**: confirm `java -version`, ensure Java is on `PATH`,
  and check whether local loopback sockets are restricted by the environment.
- **JDBC driver not found**: place the configured JAR in `jars/` or update
  `JDBC_JAR_PATH` in `.env`.
- **PostgreSQL authentication failure**: confirm `docker ps`, the mapped port,
  and `DB_*` values. Never replace the password with a hardcoded Python value.
- **Airflow port 8080 already in use**: stop the older Airflow process before
  starting another standalone instance; do not run two Project 7 publishers.
- **DAG is missing**: start Airflow through `scripts/run_airflow.sh`, which sets
  the Project 7 `AIRFLOW_HOME` and DAG folder, then inspect DAG import errors.
- **Batch ID already exists**: immutable Bronze rejected a duplicate identifier.
  Use a new batch ID unless intentionally retrying through the same Airflow run.
- **Quality or load failure**: correct the underlying source/configuration issue
  and retry the failed run. Do not manually advance the watermark.

For local development, generated Parquet, Airflow metadata, and state can be
recreated, but removing them starts a new pipeline history and should be an
explicit decision—not a routine recovery step.

## Interview explanation

A concise explanation of this project is:

> I built an incremental PySpark pipeline over a PostgreSQL retail source. It
> extracts changes with a compound timestamp/key watermark into immutable
> Bronze Parquet, validates and deduplicates them into a Silver current-state
> snapshot, creates reconciled Gold business aggregates, and atomically serves
> those aggregates back to PostgreSQL. Airflow handles branching, retries, and
> task visibility. Invalid rows go to quarantine, and the watermark advances
> only after cross-layer quality checks succeed.

Concepts demonstrated include medallion architecture, incremental extraction,
late updates, deterministic deduplication with window functions, Parquet
partitioning, lazy evaluation, caching, shuffle management, AQE, JDBC staging,
transactional publication, data reconciliation, quarantine design,
idempotency, observability, and orchestration.

### Production evolution

This is deliberately a single-machine learning implementation. At larger
scale, move lake storage to S3/ADLS/GCS, use a transactional table format such
as Delta Lake or Iceberg, store the watermark in a transactional metadata
store, use Airflow connections or a secrets manager, run Spark on a cluster,
replace single-file snapshot output with size-based partitioning/compaction,
and add metrics, alerting, schema-evolution controls, CI, and integration tests.

## Verified status

Through Step 14, the changed-data and no-change Airflow paths have succeeded,
all ten cross-layer quality checks pass, and the automated suite reports four
passing PySpark tests. The verified Silver snapshot contains 12 valid orders
with revenue `333700.00`; these values are test-environment observations, not
hardcoded pipeline expectations.

The final audit also verifies source connectivity through psycopg2 and Spark
JDBC, clean Airflow DAG imports, shell and Python syntax, ignored secrets and
runtime artifacts, and an isolated Project 7 `AIRFLOW_HOME` during environment
validation.
