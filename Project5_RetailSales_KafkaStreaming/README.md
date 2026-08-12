# Project 5 — Retail Sales Kafka Streaming Pipeline

A real-time retail sales data engineering pipeline built with **Apache Kafka, PySpark Structured Streaming, and PostgreSQL**.

This project demonstrates how retail sales events can be published to Kafka, processed in real time using Spark Structured Streaming, transformed, and loaded into PostgreSQL.

---

## 🚀 Project Overview

This project extends the retail sales pipeline into a streaming architecture.

Instead of processing only a static dataset through batch ETL, retail sales records are published as events to **Apache Kafka** and consumed by **PySpark Structured Streaming**.

The streaming pipeline:

1. Reads retail sales events from Kafka.
2. Converts Kafka JSON messages into structured Spark DataFrames.
3. Calculates `total_amount`.
4. Processes streaming data using micro-batches.
5. Uses Spark checkpointing for recovery.
6. Loads processed records into PostgreSQL.
7. Uses PostgreSQL upsert logic to handle duplicate `order_id` values.

---

## 🏗️ Architecture

```text
                 Retail Sales Data
                        │
                        ▼
                  sales.csv
                        │
                        ▼
                Kafka Producer
                        │
                        ▼
                Apache Kafka
                  retail_sales
                        │
                        ▼
          Spark Structured Streaming
                        │
             ┌──────────┴──────────┐
             │                     │
             ▼                     ▼
        JSON Parsing        Transformation
                                   │
                                   ▼
                       total_amount = price
                                      × quantity
                                   │
                         ┌─────────┴─────────┐
                         │                   │
                         ▼                   ▼
                    PostgreSQL           Parquet
                  streaming_sales         output/
```

---

## 🛠️ Technologies

| Technology | Purpose |
|------------|---------|
| Python | Application development |
| Apache Kafka | Real-time event streaming |
| PySpark | Stream processing |
| Spark Structured Streaming | Real-time data processing |
| PostgreSQL | Target database |
| psycopg2 | PostgreSQL connectivity |
| Docker | Infrastructure |
| python-dotenv | Environment variable management |
| Git & GitHub | Version control |

---

## 📂 Project Structure

```text
Project5_RetailSales_KafkaStreaming/
│
├── data/
│   └── sales.csv
│
├── scripts/
│   ├── producer.py
│   ├── consumer.py
│   ├── streaming_etl.py
│   ├── streaming_to_postgres.py
│   ├── config.py
│   ├── check_output.py
│   ├── test_db.py
│   └── test_logger.py
│
├── sql/
│   └── schema.sql
│
├── utils/
│   ├── __init__.py
│   └── logger.py
│
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

Generated files such as checkpoints, logs, Spark output, virtual environments, secrets, and JDBC JAR files are excluded from Git tracking.

---

## 📊 Source Data

The source dataset is:

```text
data/sales.csv
```

The sales data contains the following fields:

| Column | Description |
|--------|-------------|
| `order_id` | Order identifier |
| `customer_name` | Customer name |
| `product` | Product name |
| `category` | Product category |
| `price` | Product price |
| `quantity` | Quantity purchased |
| `city` | Customer city |

---

## 🔄 Streaming Data Transformation

Kafka messages are received as JSON.

Spark Structured Streaming parses the JSON data using a defined schema.

The pipeline then calculates:

```text
total_amount = price × quantity
```

For example:

```text
price = 85000
quantity = 1

total_amount = 85000
```

---

## 📨 Kafka

The Kafka topic used by the project is:

```text
retail_sales
```

The Kafka broker is configured as:

```text
localhost:9092
```

The producer publishes retail sales records to the Kafka topic.

---

## ⚡ Spark Structured Streaming

The main PostgreSQL streaming application is:

```text
scripts/streaming_to_postgres.py
```

The application:

- connects to Kafka
- reads streaming messages
- converts Kafka values from bytes to strings
- parses JSON
- applies the sales schema
- calculates `total_amount`
- processes micro-batches using `foreachBatch`
- writes processed records to PostgreSQL
- uses Spark checkpointing

---

## 🐘 PostgreSQL

Processed streaming records are stored in:

```text
streaming_sales
```

The table contains:

| Column | Type |
|--------|------|
| `order_id` | INTEGER |
| `customer_name` | VARCHAR |
| `product` | VARCHAR |
| `category` | VARCHAR |
| `price` | INTEGER |
| `quantity` | INTEGER |
| `city` | VARCHAR |
| `total_amount` | INTEGER |

The table schema is defined in:

```text
sql/schema.sql
```

### Upsert Logic

The PostgreSQL streaming pipeline uses:

```sql
ON CONFLICT (order_id)
DO UPDATE
```

This prevents duplicate `order_id` records from being inserted and updates the existing record instead.

---

## 🔐 Environment Variables

Database credentials are stored in a local `.env` file.

Create:

```text
Project5_RetailSales_KafkaStreaming/.env
```

Example:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=retail_db
DB_USER=your_database_user
DB_PASSWORD=your_database_password
```

The `.env` file is ignored by Git and should **never be committed to GitHub**.

---

## ⚙️ Installation

### 1. Clone the repository

```bash
git clone https://github.com/Dipti-Coder202/DataEngineering.git
```

### 2. Navigate to the project

```bash
cd DataEngineering/Project5_RetailSales_KafkaStreaming
```

### 3. Create a virtual environment

```bash
python3 -m venv venv
```

### 4. Activate the virtual environment

```bash
source venv/bin/activate
```

### 5. Install dependencies

```bash
pip install -r requirements.txt
```

---

## 🐳 Start Infrastructure

Start the Docker services:

```bash
docker compose up -d
```

Check running containers:

```bash
docker ps
```

---

## 🗄️ Create PostgreSQL Table

Connect to PostgreSQL:

```bash
psql -U retail_user -d retail_db
```

Then run:

```sql
CREATE TABLE IF NOT EXISTS streaming_sales (
    order_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(100),
    product VARCHAR(100),
    category VARCHAR(100),
    price INTEGER,
    quantity INTEGER,
    city VARCHAR(100),
    total_amount INTEGER
);
```

Alternatively, from the **Mac Terminal** you can execute:

```bash
psql -U retail_user -d retail_db \
    -f sql/schema.sql
```

> The `psql -f` command must be run from the Terminal, not from inside the `retail_db=>` PostgreSQL prompt.

---

## 📨 Run Kafka Producer

From the project directory:

```bash
python scripts/producer.py
```

The producer publishes retail sales events to:

```text
retail_sales
```

---

## ⚡ Run Spark Streaming → PostgreSQL

Start the streaming application:

```bash
python scripts/streaming_to_postgres.py
```

The application continuously reads Kafka events and loads the processed records into PostgreSQL.

---

## 🔎 Verify PostgreSQL Data

Connect to PostgreSQL:

```bash
psql -U retail_user -d retail_db
```

Check the records:

```sql
SELECT *
FROM streaming_sales
ORDER BY order_id;
```

Check the number of records:

```sql
SELECT COUNT(*)
FROM streaming_sales;
```

Check for duplicate order IDs:

```sql
SELECT order_id, COUNT(*)
FROM streaming_sales
GROUP BY order_id
HAVING COUNT(*) > 1;
```

Because `order_id` is the primary key, the duplicate query should return no rows.

---

## 💾 Spark Checkpointing

The streaming application uses Spark checkpointing to maintain streaming progress and support recovery.

Checkpoint files are stored locally under:

```text
checkpoints/
```

The PostgreSQL streaming pipeline uses:

```text
checkpoints/postgres/
```

Checkpoint files are generated runtime artifacts and are excluded from Git.

---

## 📁 Alternative Streaming ETL

The project also contains:

```text
scripts/streaming_etl.py
```

This Spark Structured Streaming pipeline reads data from Kafka, performs the streaming transformation, and writes the processed data to Parquet.

Generated Parquet output is stored locally under:

```text
output/
```

The generated output is excluded from Git.

---

## 🧪 Testing

### Test PostgreSQL connection

```bash
python scripts/test_db.py
```

### Test logging

```bash
python scripts/test_logger.py
```

### Check generated Spark output

```bash
python scripts/check_output.py
```

---

## 📈 Example PostgreSQL Result

Example records processed by the pipeline:

| order_id | customer | product | quantity | total_amount |
|----------|----------|---------|----------|--------------|
| 1001 | Alice | Laptop | 1 | 85000 |
| 1002 | Bob | Mouse | 2 | 1000 |
| 1003 | Charlie | Keyboard | 1 | 1200 |
| 1004 | David | Chair | 2 | 7000 |
| 1005 | Eva | Table | 1 | 7000 |

---

## 🎯 Data Engineering Concepts Demonstrated

This project demonstrates:

- Event-driven data pipelines
- Apache Kafka
- Kafka topics
- Kafka producers and consumers
- Spark Structured Streaming
- JSON parsing
- Schema definition
- Streaming transformations
- Micro-batch processing
- `foreachBatch`
- PostgreSQL integration
- PostgreSQL upsert logic
- Spark checkpointing
- Environment variable management
- Logging
- Docker
- Git and GitHub

---

## 🚀 Future Improvements

Potential improvements include:

- Add data quality validation
- Add handling for malformed Kafka messages
- Add a dead-letter queue
- Add Kafka consumer groups
- Add monitoring and metrics
- Add Apache Airflow orchestration
- Add Delta Lake
- Add AWS S3
- Add Databricks deployment
- Add automated CI/CD testing
- Add a real-time dashboard

---

## 👩‍💻 Author

**Dipti Sahu**

Data Engineering Portfolio

Built as part of a progressive retail data engineering project series using:

**Python · SQL · Kafka · Spark · PostgreSQL · Docker · GitHub**