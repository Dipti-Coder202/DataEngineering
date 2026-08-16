CREATE TABLE IF NOT EXISTS public.analytics_overall_sales (
    metric_scope VARCHAR(50) PRIMARY KEY,
    total_revenue NUMERIC(20, 2) NOT NULL,
    order_count BIGINT NOT NULL,
    units_sold BIGINT NOT NULL,
    average_order_value NUMERIC(20, 2) NOT NULL,
    distinct_customers BIGINT NOT NULL,
    last_refreshed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS public.analytics_category_sales (
    category VARCHAR(100) PRIMARY KEY,
    total_revenue NUMERIC(20, 2) NOT NULL,
    order_count BIGINT NOT NULL,
    units_sold BIGINT NOT NULL,
    average_order_value NUMERIC(20, 2) NOT NULL,
    distinct_customers BIGINT NOT NULL,
    last_refreshed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS public.analytics_city_sales (
    city VARCHAR(100) PRIMARY KEY,
    total_revenue NUMERIC(20, 2) NOT NULL,
    order_count BIGINT NOT NULL,
    units_sold BIGINT NOT NULL,
    average_order_value NUMERIC(20, 2) NOT NULL,
    distinct_customers BIGINT NOT NULL,
    last_refreshed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS public.analytics_product_sales (
    product VARCHAR(100) NOT NULL,
    category VARCHAR(100) NOT NULL,
    total_revenue NUMERIC(20, 2) NOT NULL,
    order_count BIGINT NOT NULL,
    units_sold BIGINT NOT NULL,
    average_order_value NUMERIC(20, 2) NOT NULL,
    distinct_customers BIGINT NOT NULL,
    last_refreshed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (product, category)
);

CREATE TABLE IF NOT EXISTS public.analytics_top_products (
    sales_rank INTEGER PRIMARY KEY,
    product VARCHAR(100) NOT NULL,
    category VARCHAR(100) NOT NULL,
    total_revenue NUMERIC(20, 2) NOT NULL,
    order_count BIGINT NOT NULL,
    units_sold BIGINT NOT NULL,
    average_order_value NUMERIC(20, 2) NOT NULL,
    last_refreshed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS public.analytics_customer_sales (
    customer_name VARCHAR(100) PRIMARY KEY,
    total_revenue NUMERIC(20, 2) NOT NULL,
    order_count BIGINT NOT NULL,
    units_sold BIGINT NOT NULL,
    average_order_value NUMERIC(20, 2) NOT NULL,
    distinct_customers BIGINT NOT NULL,
    last_refreshed_at TIMESTAMPTZ NOT NULL,
    distinct_products BIGINT NOT NULL
);
