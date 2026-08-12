# Project 5 — Retail Sales Kafka Streaming Pipeline

A real-time retail sales data engineering pipeline built with **Python, Apache Kafka, PySpark Structured Streaming, PostgreSQL, Docker, and GitHub**.

This project extends a retail sales batch-processing pipeline into a streaming architecture. Retail sales records are published as events to **Apache Kafka**, processed using **Spark Structured Streaming**, transformed in real time, and loaded into **PostgreSQL**.

---

## 🚀 Project Overview

The pipeline processes retail sales events through the following flow:

1. Reads retail sales records from `sales.csv`.
2. Publishes each sales record as a JSON event to Kafka.
3. Stores events in the `retail_sales` Kafka topic.
4. Reads Kafka events using PySpark Structured Streaming.
5. Parses the JSON messages using a defined schema.
6. Calculates `total_amount` using price and quantity.
7. Processes records using Spark micro-batches and `foreachBatch`.
8. Loads processed records into PostgreSQL.
9. Uses PostgreSQL upsert logic to handle duplicate `order_id` values.
10. Uses Spark checkpointing to support streaming recovery.

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
                    JSON Parsing
                           │
                           ▼
                   Data Transformation
                           │
                           ▼
             total_amount = price × quantity
                           │
                  ┌────────┴────────┐
                  │                 │
                  ▼                 ▼
             PostgreSQL          Parquet
          streaming_sales       output/
```

---

## 🛠️ Technologies

| Technology                 | Purpose                             |
| -------------------------- | ----------------------------------- |
| Python                     | Application development             |
| Apache Kafka               | Real-time event streaming           |
| PySpark                    | Distributed data processing         |
| Spark Structured Streaming | Real-time stream processing         |
| PostgreSQL                 | Target database                     |
| psycopg2                   | PostgreSQL connectivity             |
| Docker                     | Kafka and infrastructure management |
| python-dotenv              | Environment variable management     |
| Git & GitHub               | Version control                     |

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

Runtime-generated files such as checkpoints, logs, Spark output, virtual environments, secrets, and JDBC JAR files are excluded from Git tracking.

---

## 📊 Source Data

The source dataset is:

```text
data/sales.csv
```

The dataset contains the following fields:

| Column          | Description             |
| --------------- | ----------------------- |
| `order_id`      | Unique order identifier |
| `customer_name` | Customer name           |
| `product`       | Product name            |
| `category`      | Product category        |
| `price`         | Product price           |
| `quantity`      | Quantity purchased      |
| `city`          | Customer city           |

---

## 🔄 Streaming Data Transformation

Kafka messages are published as JSON records.

Spark Structured Streaming reads the Kafka messages and converts the JSON data into a structured DataFrame.

The pipeline calculates:

```text
total_amount = price × quantity
```

Example:

```text
price = 85000
quantity = 1

total_amount = 85000
```

Another example:

```text
price = 500
quantity = 2

total_amount = 1000
```

---

## 📨 Kafka

The Kafka topic used by this project is:

```text
retail_sales
```

The Kafka broker is configured as:

```text
localhost:9092
```

The producer script is:

```text
scripts/producer.py
```

The producer reads records from:

```text
data/sales.csv
```

and publishes them as Kafka events.

---

## ⚡ Spark Structured Streaming

The main streaming application that loads data into PostgreSQL is:

```text
scripts/streaming_to_postgres.py
```

The application:

* connects to Kafka
* reads messages from the `retail_sales` topic
* converts Kafka message values from bytes to strings
* parses JSON messages
* applies the sales schema
* calculates `total_amount`
* processes micro-batches using `foreachBatch`
* writes records to PostgreSQL
* uses Spark checkpointing

---

## 🐘 PostgreSQL

Processed streaming records are stored in:

```text
streaming_sales
```

The table contains:

| Column          | Type    |
| --------------- | ------- |
| `order_id`      | INTEGER |
| `customer_name` | VARCHAR |
| `product`       | VARCHAR |
| `category`      | VARCHAR |
| `price`         | INTEGER |
| `quantity`      | INTEGER |
| `city`          | VARCHAR |
| `total_amount`  | INTEGER |

The table definition is stored in:

```text
sql/schema.sql
```

### Upsert Logic

The PostgreSQL pipeline uses:

```sql
ON CONFLICT (order_id)
DO UPDATE
```

This allows the pipeline to update an existing record when the same `order_id` is processed again instead of creating a duplicate row.

---

## 🔐 Environment Variables

Database configuration is stored in a local `.env` file.

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

Do not place real passwords in the README.

The `.env` file should be included in `.gitignore` and must **never be committed to GitHub**.

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

## 🐳 Start Kafka Infrastructure

Start the Kafka and ZooKeeper containers:

```bash
docker compose up -d
```

Check the running containers:

```bash
docker ps
```

The project uses:

```text
Kafka      → localhost:9092
ZooKeeper  → localhost:2181
```

---

## 📨 Verify Kafka Topic

The Kafka topic used by the project is:

```text
retail_sales
```

You can verify the topic from the Kafka container:

```bash
docker exec -it project5_retailsales_kafkastreaming-kafka-1 \
kafka-topics --bootstrap-server localhost:9092 --list
```

Expected output should include:

```text
retail_sales
```

---

## 🗄️ Create PostgreSQL Table

Make sure PostgreSQL is running and the database exists.

Connect using:

```bash
psql -U retail_user -d retail_db
```

Then create the table:

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

The SQL schema is also available in:

```text
sql/schema.sql
```

From the **Mac Terminal**, you can run:

```bash
psql -U retail_user -d retail_db -f sql/schema.sql
```

> Run the `psql -f` command from the project directory in the Mac Terminal. Do not run it from inside the `retail_db=>` PostgreSQL prompt.

---

## 📨 Run Kafka Producer

From the project directory:

```bash
python scripts/producer.py
```

The producer reads:

```text
data/sales.csv
```

and publishes the sales records to:

```text
retail_sales
```

Expected output is similar to:

```text
Sent: 1001
Sent: 1002
Sent: 1003
...
Sent: 1010
Finished Sending Data
```

---

## ⚡ Run Spark Streaming → PostgreSQL

Start the Spark streaming application:

```bash
python scripts/streaming_to_postgres.py
```

The application continuously reads Kafka events and processes them using Spark Structured Streaming.

Processed records are written to:

```text
PostgreSQL → streaming_sales
```

Keep the streaming application running while Kafka events are being processed.

---

## 🔎 Verify PostgreSQL Data

Connect to PostgreSQL:

```bash
psql -U retail_user -d retail_db
```

Check the processed records:

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

Checkpoint files are generated locally under:

```text
checkpoints/
```

The PostgreSQL streaming application uses:

```text
checkpoints/postgres/
```

Checkpoint files are runtime artifacts and should not be committed to Git.

---

## 📁 Alternative Streaming ETL

The project also contains:

```text
scripts/streaming_etl.py
```

This application reads Kafka events using Spark Structured Streaming, performs the streaming transformation, and writes processed data to Parquet.

Generated Parquet files are stored under:

```text
output/
```

The generated output is excluded from Git tracking.

---

## 🧪 Testing

### Test PostgreSQL Connection

Run:

```bash
python scripts/test_db.py
```

This verifies the PostgreSQL database connection.

### Test Logging

Run:

```bash
python scripts/test_logger.py
```

This verifies the project's logging functionality.

### Check Spark Output

Run:

```bash
python scripts/check_output.py
```

This checks the generated Spark output.

---

## 📈 Example PostgreSQL Result

Example processed records:

| order_id | customer | product  | quantity | total_amount |
| -------: | -------- | -------- | -------: | -----------: |
|     1001 | Alice    | Laptop   |        1 |        85000 |
|     1002 | Bob      | Mouse    |        2 |         1000 |
|     1003 | Charlie  | Keyboard |        1 |         1200 |
|     1004 | David    | Chair    |        2 |         7000 |
|     1005 | Eva      | Table    |        1 |         7000 |

---

## 🎯 Data Engineering Concepts Demonstrated

This project demonstrates:

* Event-driven data pipelines
* Apache Kafka
* Kafka topics
* Kafka producers and consumers
* JSON event processing
* PySpark
* Spark Structured Streaming
* Schema definition
* Streaming transformations
* Micro-batch processing
* `foreachBatch`
* PostgreSQL integration
* PostgreSQL upsert logic
* Spark checkpointing
* Environment variable management
* Logging
* Docker
* Git and GitHub

---

## 🚀 Future Improvements

Potential improvements include:

* Add data quality validation
* Handle malformed Kafka messages
* Add a dead-letter queue
* Add Kafka consumer groups
* Add monitoring and metrics
* Add Apache Airflow orchestration
* Add Delta Lake
* Add AWS S3
* Add Databricks deployment
* Add automated CI/CD testing
* Add a real-time dashboard

---

## 👩‍💻 Author

**Dipti Sahu**

Data Engineering Portfolio

Built as part of a progressive retail data engineering project series using:

**Python · SQL · Kafka · Spark · PostgreSQL · Docker · GitHub**
