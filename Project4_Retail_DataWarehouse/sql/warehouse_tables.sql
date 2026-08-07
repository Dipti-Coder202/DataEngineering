CREATE TABLE dim_customer(
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) UNIQUE
);

CREATE TABLE dim_product(
    product_id SERIAL PRIMARY KEY,
    product VARCHAR(100),
    category VARCHAR(100)
);

CREATE TABLE dim_city(
    city_id SERIAL PRIMARY KEY,
    city VARCHAR(100)
);

CREATE TABLE fact_sales(
    sales_id SERIAL PRIMARY KEY,
    order_id INT,
    customer_id INT,
    product_id INT,
    city_id INT,
    price INT,
    quantity INT
);