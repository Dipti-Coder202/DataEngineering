import os
from datetime import datetime
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_DIR / ".env")


DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "retail_db")
DB_USER = os.getenv("DB_USER", "retail_user")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def run_etl():
    if not DB_PASSWORD:
        raise ValueError("DB_PASSWORD is missing from Project6_RetailSales_Airflow/.env")

    # The context managers commit on success, roll back on failure, and always
    # close the database resources.
    with psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=10,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT to_regclass('public.retail_sales')")
            source_table_exists = cursor.fetchone()[0] is not None

            if not source_table_exists:
                cursor.execute("""
                    CREATE TABLE retail_sales (
                        order_id INTEGER PRIMARY KEY,
                        customer_name VARCHAR(100),
                        product VARCHAR(100),
                        category VARCHAR(100),
                        price INTEGER,
                        quantity INTEGER,
                        city VARCHAR(100)
                    );
                """)
                with (PROJECT_DIR / "data" / "sales.csv").open() as sales_file:
                    cursor.copy_expert(
                        "COPY retail_sales FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
                        sales_file,
                    )

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS airflow_sales (
                    order_id INTEGER PRIMARY KEY,
                    customer_name VARCHAR(100),
                    product VARCHAR(100),
                    category VARCHAR(100),
                    price INTEGER,
                    quantity INTEGER,
                    city VARCHAR(100),
                    total_amount INTEGER
                );
            """)

            cursor.execute("""
                INSERT INTO airflow_sales (
                    order_id,
                    customer_name,
                    product,
                    category,
                    price,
                    quantity,
                    city,
                    total_amount
                )
                SELECT
                    order_id,
                    customer_name,
                    product,
                    category,
                    price,
                    quantity,
                    city,
                    price * quantity AS total_amount
                FROM retail_sales
                ON CONFLICT (order_id)
                DO UPDATE SET
                    customer_name = EXCLUDED.customer_name,
                    product = EXCLUDED.product,
                    category = EXCLUDED.category,
                    price = EXCLUDED.price,
                    quantity = EXCLUDED.quantity,
                    city = EXCLUDED.city,
                    total_amount = EXCLUDED.total_amount;
            """)

    print("Retail sales ETL completed successfully.")


# Airflow DAG definition
with DAG(
    dag_id="retail_sales_pipeline",
    start_date=datetime(2026, 8, 14),
    schedule=None,
    catchup=False,
    tags=["retail", "postgres", "etl"],
) as dag:

    run_retail_etl = PythonOperator(
        task_id="run_retail_etl",
        python_callable=run_etl,
    )
