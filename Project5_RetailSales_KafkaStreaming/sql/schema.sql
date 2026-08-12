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
