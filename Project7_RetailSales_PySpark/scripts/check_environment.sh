#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPOSITORY_DIR="$(cd "$PROJECT_DIR/.." && pwd)"
PYTHON_BIN="$REPOSITORY_DIR/venv/bin/python"

# Keep validation isolated from any user-level or other-project Airflow state.
export AIRFLOW_HOME="$PROJECT_DIR/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_DIR/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "Missing repository virtual environment: $PYTHON_BIN" >&2
    exit 1
fi

command -v java >/dev/null || {
    echo "Java is required but was not found on PATH." >&2
    exit 1
}

"$PYTHON_BIN" -c "import airflow, dotenv, psycopg2, pyspark"
"$PYTHON_BIN" "$PROJECT_DIR/run_pipeline.py" --check-config

echo "Environment and Project 7 configuration checks passed."
