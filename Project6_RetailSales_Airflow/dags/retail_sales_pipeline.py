import os

import psycopg2
from dotenv import load_dotenv


load_dotenv()


DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "retail_db")
DB_USER = os.getenv("DB_USER", "retail_user")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def run_etl():
    connection = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

    cursor = connection.cursor()

    # Create target table
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

    # Load data from retail_sales into airflow_sales
    # and update existing records when order_id already exists.
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

    connection.commit()

    cursor.close()
    connection.close()

    print("Retail sales ETL completed successfully.")


if __name__ == "__main__":
    run_etl()