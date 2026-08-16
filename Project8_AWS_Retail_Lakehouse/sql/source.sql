CREATE TABLE IF NOT EXISTS retail_sales (
    order_id BIGINT PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    product VARCHAR(100) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price NUMERIC(12, 2) NOT NULL CHECK (price >= 0),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    city VARCHAR(100) NOT NULL,
    order_date DATE NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO retail_sales
    (order_id, customer_name, product, category, price, quantity, city, order_date)
VALUES
    (1, 'Asha', 'Laptop', 'Electronics', 65000, 1, 'Pune', '2026-08-01'),
    (2, 'Ravi', 'Phone', 'Electronics', 30000, 2, 'Mumbai', '2026-08-02'),
    (3, 'Nina', 'Desk', 'Furniture', 12000, 1, 'Bengaluru', '2026-08-02'),
    (4, 'Vikram', 'Chair', 'Furniture', 7000, 2, 'Delhi', '2026-08-03')
ON CONFLICT (order_id) DO NOTHING;

CREATE OR REPLACE FUNCTION set_retail_sales_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS retail_sales_updated_at_trigger ON retail_sales;
CREATE TRIGGER retail_sales_updated_at_trigger
BEFORE UPDATE ON retail_sales
FOR EACH ROW EXECUTE FUNCTION set_retail_sales_updated_at();
