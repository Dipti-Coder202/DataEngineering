# Project 6: Retail Sales Airflow

This project runs the `retail_sales_pipeline` DAG against the PostgreSQL
database configured in `.env`.

From the repository root, start Airflow with:

```bash
./Project6_RetailSales_Airflow/start_airflow.sh
```

Then open <http://localhost:8080>, enable `retail_sales_pipeline`, and trigger
it manually. PostgreSQL must be running. If the source table `retail_sales`
does not exist, the task seeds it from `data/sales.csv`; it never replaces an
existing source table.

The launcher deliberately keeps Airflow's metadata under `airflow/`, points
Airflow at this project's sibling `dags/` directory, and disables the bundled
example DAGs.
