-- Create database
CREATE DATABASE partitioning_test;
CREATE DATABASE analytical;
CREATE DATABASE json_demo;



-- Connect to the partitioning_test database
\connect partitioning_test

-- Create schema for partman
CREATE SCHEMA partman;

-- Create extensions (pg_cron now works since we set cron.database_name = 'partitioning_test')
CREATE EXTENSION IF NOT EXISTS pg_partman with schema partman;
CREATE EXTENSION IF NOT EXISTS pg_cron;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS bloom;
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_ivm;
CREATE EXTENSION IF NOT EXISTS citus;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;


-- Set search path
ALTER DATABASE partitioning_test SET search_path = public, partman;
SET search_path = public, partman;

-- Create tables
CREATE TABLE table_a (
    "timestamp" TIMESTAMP WITH TIME ZONE NOT NULL,
    id UUID NOT NULL,
    data TEXT,
    PRIMARY KEY (id)
);

-- Insert sample data
INSERT INTO table_a
SELECT
  NOW() - (i || ' days')::INTERVAL,
  gen_random_uuid(),
  'Sample data ' || i
FROM generate_series(1, 10000) i;

CREATE TABLE table_b_partitioned (
    "timestamp" TIMESTAMP WITH TIME ZONE NOT NULL,
    id UUID NOT NULL,
    data TEXT,
    PRIMARY KEY (id, "timestamp")
) PARTITION BY RANGE ("timestamp");


--- create the function to insert data into partition table
CREATE OR REPLACE FUNCTION dbre_populate_partitioned_table(
    num_records INTEGER DEFAULT 100,
    start_date TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    end_date TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    specific_date TIMESTAMP WITH TIME ZONE DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    i INTEGER := 0;
    effective_start TIMESTAMP WITH TIME ZONE;
    effective_end TIMESTAMP WITH TIME ZONE;
    random_timestamp TIMESTAMP WITH TIME ZONE;
    random_data TEXT;
    data_options TEXT[] := ARRAY['sample data 1', 'important info',
                                'critical data', 'regular update',
                                'system log', 'user activity'];
BEGIN
    -- Determine actual start/end based on specific_date or default relative range
    IF specific_date IS NOT NULL THEN
        effective_start := specific_date - INTERVAL '12 hours';
        effective_end := specific_date + INTERVAL '12 hours';
    ELSE
        effective_start := COALESCE(start_date, NOW() - INTERVAL '3 months');
        effective_end := COALESCE(end_date, NOW());
    END IF;

    FOR i IN 1..num_records LOOP
        random_timestamp := effective_start + (random() * (effective_end - effective_start));
        random_data := data_options[1 + floor(random() * array_length(data_options, 1))::integer];

        INSERT INTO table_b_partitioned (timestamp, id, data)
        VALUES (
            random_timestamp,
            gen_random_uuid(),
            random_data || ' - ' || to_char(random_timestamp, 'YYYY-MM-DD HH24:MI:SS')
        );
    END LOOP;

    RETURN num_records;
END;
$$ LANGUAGE plpgsql;

-- Use the correct named parameter syntax with => instead of :=
SELECT partman.create_parent(
    p_parent_table => 'public.table_b_partitioned',
    p_control => 'timestamp',
    p_type => 'range',
    p_interval => '7 Days',
    p_premake => 3,
    p_start_partition => date_trunc('week', current_date - interval '3 months')::text,
    p_default_table => FALSE
);

-- Drop and recreate earthquakes table
DROP TABLE IF EXISTS earthquakes;
CREATE TABLE earthquakes (
    id SERIAL PRIMARY KEY,
    content JSONB
);

-- Insert earthquake JSON data with nested location
INSERT INTO earthquakes (content) VALUES (
'{
  "features": [
    {
      "properties": {
        "title": "M 1.2 - 5km SW of Anza, CA",
        "mag": 1.2,
        "place": "5km SW of Anza, CA",
        "time": "2025-04-21T00:00:00Z",
        "url": "http://example.com/ci1",
        "detail": "http://example.com/detail/ci1",
        "felt": 2,
        "location": { "lat": 33.555, "lon": -116.673 }
      },
      "id": "ci1"
    },
    {
      "properties": {
        "title": "M 3.5 - 10km SE of Ridgecrest, CA",
        "mag": 3.5,
        "place": "10km SE of Ridgecrest, CA",
        "time": "2025-04-20T10:00:00Z",
        "url": "http://example.com/ci2",
        "detail": "http://example.com/detail/ci2",
        "felt": 5,
        "location": { "lat": 35.622, "lon": -117.67 }
      },
      "id": "ci2"
    }
  ]
}'
);

-- Drop and recreate regions table
DROP TABLE IF EXISTS regions;
CREATE TABLE regions (
    region_id SERIAL PRIMARY KEY,
    region_name TEXT,
    lat_min REAL,
    lat_max REAL,
    lon_min REAL,
    lon_max REAL
);

-- Insert regions
INSERT INTO regions (region_name, lat_min, lat_max, lon_min, lon_max) VALUES
('Southern California', 32.0, 35.0, -118.0, -114.0),
('Northern California', 35.0, 42.0, -124.0, -118.0),
('Alaska', 54.0, 72.0, -170.0, -130.0),
('Nevada', 35.0, 42.0, -120.0, -114.0),
('Japan', 30.0, 45.0, 129.0, 146.0),
('Chile', -56.0, -17.0, -75.0, -66.0),
('Idaho', 42.0, 49.0, -117.0, -111.0),
('Montana', 44.0, 49.0, -116.0, -104.0),
('Oregon', 42.0, 46.5, -124.5, -116.5),
('New Zealand', -47.0, -34.0, 166.0, 179.0);


\connect analytical

CREATE EXTENSION IF NOT EXISTS pg_partman;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS bloom;
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- ============================
-- DIMENSION TABLES
-- ============================
DROP TABLE IF EXISTS fact_transactions CASCADE;
DROP TABLE IF EXISTS fact_user_events CASCADE;
DROP TABLE IF EXISTS dim_campaigns CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;
DROP TABLE IF EXISTS dim_users CASCADE;
DROP TABLE IF EXISTS dim_dates CASCADE;

CREATE TABLE dim_dates (
    date_id DATE PRIMARY KEY,
    day_of_week SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL,
    year SMALLINT NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE
);

CREATE TABLE dim_users (
    user_id SERIAL PRIMARY KEY,
    user_segment TEXT NOT NULL,
    country_code CHAR(2) NOT NULL,
    signup_date DATE REFERENCES dim_dates(date_id),
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE dim_products (
    product_id SERIAL PRIMARY KEY,
    category TEXT NOT NULL,
    subcategory TEXT NOT NULL,
    price_tier TEXT NOT NULL CHECK (price_tier IN ('basic', 'premium')),
    is_featured BOOLEAN DEFAULT FALSE
);

CREATE TABLE dim_campaigns (
    campaign_id SERIAL PRIMARY KEY,
    campaign_type TEXT NOT NULL,
    channel TEXT NOT NULL,
    start_date DATE NOT NULL REFERENCES dim_dates(date_id),
    end_date DATE NOT NULL REFERENCES dim_dates(date_id),
    CHECK (start_date <= end_date)
);

-- ============================
-- FACT TABLES
-- ============================
CREATE TABLE fact_user_events (
    event_id BIGSERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES dim_users(user_id),
    event_date DATE NOT NULL REFERENCES dim_dates(date_id),
    event_type TEXT NOT NULL CHECK (event_type IN ('purchase', 'add_to_cart', 'product_view', 'search')),
    platform TEXT NOT NULL CHECK (platform IN ('web', 'mobile')),
    session_id UUID NOT NULL,
    page_path TEXT
);

CREATE TABLE fact_transactions (
    transaction_id BIGSERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES dim_users(user_id),
    product_id INTEGER NOT NULL REFERENCES dim_products(product_id),
    campaign_id INTEGER NOT NULL REFERENCES dim_campaigns(campaign_id),
    transaction_date DATE NOT NULL REFERENCES dim_dates(date_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    revenue NUMERIC(10,2) NOT NULL,
    discount_amount NUMERIC(10,2) DEFAULT 0
);

-- ============================
-- FUNCTIONS
-- ============================

CREATE OR REPLACE FUNCTION public.dbre_cleanup_all_data()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
    -- Delete data from fact tables first to avoid foreign key conflicts
    TRUNCATE TABLE fact_user_events CASCADE;
    TRUNCATE TABLE fact_transactions CASCADE;

    -- Then clear dimension tables
    TRUNCATE TABLE dim_campaigns CASCADE;
    TRUNCATE TABLE dim_products CASCADE;
    TRUNCATE TABLE dim_users CASCADE;
    TRUNCATE TABLE dim_dates CASCADE;

    RAISE NOTICE 'All tables have been cleared successfully';
END;
$function$;

CREATE OR REPLACE FUNCTION dbre_generate_high_volume_data()
RETURNS void AS $$
DECLARE
    i INT;
    batch_size INT := 1000;
    total_batches INT;
    batch_num INT;
    d DATE;
    start_base DATE := DATE '2023-01-01';
    end_base DATE := DATE '2023-12-31';
    categories TEXT[] := ARRAY['electronics', 'clothing', 'home', 'books', 'beauty', 'sports', 'food', 'office', 'garden', 'toys'];
    subcategories TEXT[] := ARRAY['laptop', 'shirt', 'furniture', 'novel', 'cosmetics', 'equipment', 'snacks', 'supplies', 'plants', 'games'];
    price_tiers TEXT[] := ARRAY['basic', 'premium'];
    countries TEXT[] := ARRAY['US', 'IN', 'CA', 'UK', 'FR', 'DE', 'JP', 'BR', 'AU', 'MX'];
    segments TEXT[] := ARRAY['new', 'returning', 'vip', 'dormant', 'high_value'];
    channels TEXT[] := ARRAY['email', 'social', 'search', 'display', 'affiliate', 'direct', 'partner'];
    campaign_types TEXT[] := ARRAY['seasonal', 'clearance', 'new_launch', 'promotion', 'loyalty', 'reactivation'];
    platforms TEXT[] := ARRAY['web', 'mobile'];
    event_types TEXT[] := ARRAY['purchase', 'add_to_cart', 'product_view', 'search'];

    -- For safe ID retrieval
    user_ids INT[];
    product_ids INT[];
    campaign_ids INT[];
    date_ids DATE[];
    random_user_id INT;
    random_product_id INT;
    random_campaign_id INT;
    random_date DATE;
    progress_counter INT := 0;
BEGIN
    -- Clean up any existing data
    PERFORM cleanup_all_data();

    RAISE NOTICE 'Starting high volume data generation...';

    -- dim_dates - Generate one year of dates (365 days)
    RAISE NOTICE 'Generating dates...';
    FOR i IN 0..364 LOOP
        d := start_base + i;
        INSERT INTO dim_dates(date_id, day_of_week, month, quarter, year, is_holiday)
        VALUES (
            d,
            EXTRACT(DOW FROM d),
            EXTRACT(MONTH FROM d),
            EXTRACT(QUARTER FROM d),
            EXTRACT(YEAR FROM d),
            (RANDOM() < 0.1)
        );
    END LOOP;

    -- Collect date_ids for use in dim_users
    SELECT ARRAY(SELECT date_id FROM dim_dates) INTO date_ids;

    -- dim_users - Generate 5,000 users
    RAISE NOTICE 'Generating 5,000 users...';
    FOR i IN 1..5000 LOOP
        INSERT INTO dim_users(user_segment, country_code, signup_date, is_active)
        VALUES (
            segments[1 + (RANDOM() * (array_length(segments, 1)-1))::INT],
            countries[1 + (RANDOM() * (array_length(countries, 1)-1))::INT],
            date_ids[1 + (RANDOM() * (array_length(date_ids, 1)-1))::INT], -- Random date from dim_dates
            (RANDOM() > 0.15) -- 85% active users
        );

        IF i % 1000 = 0 THEN
            RAISE NOTICE '% users generated', i;
        END IF;
    END LOOP;

    -- dim_products - Generate 5,000 products
    RAISE NOTICE 'Generating 5,000 products...';
    FOR i IN 1..5000 LOOP
        INSERT INTO dim_products(category, subcategory, price_tier, is_featured)
        VALUES (
            categories[1 + (RANDOM() * (array_length(categories, 1)-1))::INT],
            subcategories[1 + (RANDOM() * (array_length(subcategories, 1)-1))::INT],
            price_tiers[1 + (RANDOM() * (array_length(price_tiers, 1)-1))::INT],
            (RANDOM() > 0.8)
        );

        IF i % 1000 = 0 THEN
            RAISE NOTICE '% products generated', i;
        END IF;
    END LOOP;

    -- dim_campaigns - Generate 200 campaigns
    RAISE NOTICE 'Generating 200 campaigns...';
    FOR i IN 1..200 LOOP
        DECLARE
            start_d DATE := date_ids[1 + (RANDOM() * (array_length(date_ids, 1)-1))::INT];
            duration INT := 5 + (RANDOM() * 60)::INT;
            end_d DATE := start_d + duration;
        BEGIN
            IF end_d > end_base THEN
                end_d := end_base;
            END IF;

            INSERT INTO dim_campaigns(campaign_type, channel, start_date, end_date)
            VALUES (
                campaign_types[1 + (RANDOM() * (array_length(campaign_types, 1)-1))::INT],
                channels[1 + (RANDOM() * (array_length(channels, 1)-1))::INT],
                start_d,
                end_d
            );
        END;
    END LOOP;

    -- Collect IDs
    RAISE NOTICE 'Collecting dimension table IDs...';
    SELECT ARRAY(SELECT user_id FROM dim_users) INTO user_ids;
    SELECT ARRAY(SELECT product_id FROM dim_products) INTO product_ids;
    SELECT ARRAY(SELECT campaign_id FROM dim_campaigns) INTO campaign_ids;

    IF array_length(user_ids, 1) IS NULL OR
       array_length(product_ids, 1) IS NULL OR
       array_length(campaign_ids, 1) IS NULL OR
       array_length(date_ids, 1) IS NULL THEN
        RAISE EXCEPTION 'One or more dimension tables are empty!';
    END IF;

    RAISE NOTICE 'Starting fact table generation in batches...';

    -- fact_transactions - Generate 1,000,000 transactions
    total_batches := 1000000 / batch_size;

    RAISE NOTICE 'Generating 1,000,000 transactions in % batches of % records each...', total_batches, batch_size;

    FOR batch_num IN 1..total_batches LOOP
        FOR i IN 1..batch_size LOOP
            random_user_id := user_ids[1 + (random() * (array_length(user_ids, 1) - 1))::int];
            random_product_id := product_ids[1 + (random() * (array_length(product_ids, 1) - 1))::int];
            random_campaign_id := campaign_ids[1 + (random() * (array_length(campaign_ids, 1) - 1))::int];
            random_date := date_ids[1 + (random() * (array_length(date_ids, 1) - 1))::int];

            INSERT INTO fact_transactions(
                user_id, product_id, campaign_id, transaction_date, quantity, revenue, discount_amount
            ) VALUES (
                random_user_id,
                random_product_id,
                random_campaign_id,
                random_date,
                1 + (RANDOM() * 10)::INT,
                (10 + (RANDOM() * 990))::NUMERIC(10,2),
                (RANDOM() * 200)::NUMERIC(10,2)
            );
        END LOOP;

        IF batch_num % 20 = 0 THEN
            progress_counter := batch_num * batch_size;
            RAISE NOTICE '% transactions generated (% complete)', progress_counter, (batch_num * 100 / total_batches) || '%';
        END IF;
    END LOOP;

    -- fact_user_events - Generate 1,000,000 user events
    RAISE NOTICE 'Generating 1,000,000 user events in % batches of % records each...', total_batches, batch_size;

    FOR batch_num IN 1..total_batches LOOP
        FOR i IN 1..batch_size LOOP
            random_user_id := user_ids[1 + (random() * (array_length(user_ids, 1) - 1))::int];
            random_date := date_ids[1 + (random() * (array_length(date_ids, 1) - 1))::int];

            INSERT INTO fact_user_events(
                user_id, event_date, event_type, platform, session_id, page_path
            ) VALUES (
                random_user_id,
                random_date,
                event_types[1 + (RANDOM() * (array_length(event_types, 1) - 1))::int],
                platforms[1 + (RANDOM() * (array_length(platforms, 1) - 1))::int],
                gen_random_uuid(),
                '/page/' || (RANDOM() * 50)::INT || '/product/' || (RANDOM() * 200)::INT
            );
        END LOOP;

        IF batch_num % 20 = 0 THEN
            progress_counter := batch_num * batch_size;
            RAISE NOTICE '% user events generated (% complete)', progress_counter, (batch_num * 100 / total_batches) || '%';
        END IF;
    END LOOP;

    RAISE NOTICE 'High volume data generation complete!';
    RAISE NOTICE 'Generated:';
    RAISE NOTICE '- % dates', array_length(date_ids, 1);
    RAISE NOTICE '- % users', array_length(user_ids, 1);
    RAISE NOTICE '- % products', array_length(product_ids, 1);
    RAISE NOTICE '- % campaigns', array_length(campaign_ids, 1);
    RAISE NOTICE '- 1,000,000 transactions';
    RAISE NOTICE '- 1,000,000 user events';

    -- Create indexes
    RAISE NOTICE 'Creating indexes for performance...';

    CREATE INDEX IF NOT EXISTS idx_fact_transactions_user_id ON fact_transactions(user_id);
    CREATE INDEX IF NOT EXISTS idx_fact_transactions_product_id ON fact_transactions(product_id);
    CREATE INDEX IF NOT EXISTS idx_fact_transactions_campaign_id ON fact_transactions(campaign_id);
    CREATE INDEX IF NOT EXISTS idx_fact_transactions_date ON fact_transactions(transaction_date);

    CREATE INDEX IF NOT EXISTS idx_fact_user_events_user_id ON fact_user_events(user_id);
    CREATE INDEX IF NOT EXISTS idx_fact_user_events_event_date ON fact_user_events(event_date);
    CREATE INDEX IF NOT EXISTS idx_fact_user_events_event_type ON fact_user_events(event_type);

    RAISE NOTICE 'Indexes created. Data generation complete!';
END;
$$ LANGUAGE plpgsql;

-- ============================
-- FUNCTION - Show count
-- ============================

CREATE OR REPLACE FUNCTION dbre_count_all_tables()
RETURNS TABLE(table_name TEXT, row_count BIGINT) AS $$
DECLARE
    tbl RECORD;
BEGIN
    FOR tbl IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
    LOOP
        EXECUTE format('SELECT %L, COUNT(*) FROM public.%I', tbl.tablename, tbl.tablename)
        INTO table_name, row_count;
        RETURN NEXT;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


-- Connect to the json_demo  database
\connect json_demo

-- Create a reset and setup script for the database to ensure clean environment
-- This is helpful for testing the query plans

-- First, drop tables if they exist
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS addresses;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS orders_jsonb;

-- Recreate normalized tables
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE addresses (
    address_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    address_type VARCHAR(10) NOT NULL, -- 'billing' or 'shipping'
    street_address VARCHAR(100) NOT NULL,
    city VARCHAR(50) NOT NULL,
    state VARCHAR(50) NOT NULL,
    postal_code VARCHAR(20) NOT NULL,
    country VARCHAR(50) NOT NULL DEFAULT 'USA',
    is_default BOOLEAN DEFAULT FALSE
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    order_status VARCHAR(20) NOT NULL DEFAULT 'pending',
    billing_address_id INTEGER REFERENCES addresses(address_id),
    shipping_address_id INTEGER REFERENCES addresses(address_id),
    total_amount NUMERIC(10,2) NOT NULL
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    description TEXT,
    price NUMERIC(10,2) NOT NULL,
    sku VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    price_at_time NUMERIC(10,2) NOT NULL,
    discount_percent NUMERIC(5,2) DEFAULT 0
);

-- Create the JSONB table
CREATE TABLE orders_jsonb (
    order_id SERIAL PRIMARY KEY,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    order_status VARCHAR(20) NOT NULL DEFAULT 'pending',
    total_amount NUMERIC(10,2) NOT NULL,
    data JSONB NOT NULL
);

-- Create baseline indexes
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_addresses_customer_id ON addresses(customer_id);
CREATE INDEX idx_orders_jsonb_customer_id ON orders_jsonb USING GIN ((data->'customer'->'customer_id'));
CREATE INDEX idx_orders_jsonb_order_date ON orders_jsonb(order_date);

-- Insert sample product data
INSERT INTO products (product_name, description, price, sku)
VALUES
('Smartphone X', 'Latest generation smartphone', 999.99, 'TECH-001'),
('Laptop Pro', 'Professional laptop for developers', 1499.99, 'TECH-002'),
('Wireless Headphones', 'Noise-cancelling headphones', 199.99, 'AUDIO-001'),
('Smart Watch', 'Fitness and health tracking', 299.99, 'WEAR-001'),
('Tablet Ultra', 'High-performance tablet', 799.99, 'TECH-003');


-- Execute the following commands once tables are populated to enable meaningful query plans
ANALYZE VERBOSE customers;
ANALYZE VERBOSE addresses;
ANALYZE VERBOSE orders;
ANALYZE VERBOSE products;
ANALYZE VERBOSE order_items;
ANALYZE VERBOSE orders_jsonb;


--- Create ingest function
CREATE OR REPLACE FUNCTION dbre_populate_orders_demo(sample_size INTEGER DEFAULT 1000)
RETURNS void AS $$
DECLARE
    i INT := 0;
    cust_id INT;
    billing_id INT;
    shipping_id INT;
    order_id INT;
    prod_id INT;
    prod_price NUMERIC(10,2);
    qty INT;
    discount NUMERIC(5,2);
    total NUMERIC(10,2);
    js JSONB;
    item JSONB;  -- Declare this for the loop over jsonb_array_elements
BEGIN
    FOR i IN 1..sample_size LOOP
        -- Create a new customer
        INSERT INTO customers (first_name, last_name, email, phone)
        VALUES (
            'First' || i,
            'Last' || i,
            'user' || i || '@example.com',
            '555-000' || i
        )
        RETURNING customer_id INTO cust_id;

        -- Create billing address
        INSERT INTO addresses (customer_id, address_type, street_address, city, state, postal_code, is_default)
        VALUES (
            cust_id, 'billing', '123 Main St', 'City' || i, 'State' || i, '000' || i, TRUE
        ) RETURNING address_id INTO billing_id;

        -- Create shipping address
        INSERT INTO addresses (customer_id, address_type, street_address, city, state, postal_code, is_default)
        VALUES (
            cust_id, 'shipping', '456 Side St', 'City' || i, 'State' || i, '111' || i, TRUE
        ) RETURNING address_id INTO shipping_id;

        -- Start with empty total and JSONB document
        total := 0;
        js := jsonb_build_object(
            'customer', jsonb_build_object(
                'customer_id', cust_id,
                'name', 'First' || i || ' Last' || i,
                'email', 'user' || i || '@example.com'
            ),
            'addresses', jsonb_build_object(
                'billing', jsonb_build_object(
                    'street_address', '123 Main St',
                    'city', 'City' || i,
                    'state', 'State' || i,
                    'postal_code', '000' || i
                ),
                'shipping', jsonb_build_object(
                    'street_address', '456 Side St',
                    'city', 'City' || i,
                    'state', 'State' || i,
                    'postal_code', '111' || i
                )
            ),
            'items', '[]'::jsonb
        );

        -- Add 1 to 5 items per order
        FOR _ IN 1..(1 + floor(random() * 5)) LOOP
            SELECT product_id, price INTO prod_id, prod_price
            FROM products
            OFFSET floor(random() * (SELECT COUNT(*) FROM products))
            LIMIT 1;

            qty := 1 + floor(random() * 5);
            discount := round((random() * 20)::numeric, 2); -- up to 20% discount, with proper numeric casting
            total := total + ((qty * prod_price) * (1 - discount / 100));

            js := jsonb_set(
                js,
                '{items}',
                (js->'items') || jsonb_build_object(
                    'product_id', prod_id,
                    'product_name', (SELECT product_name FROM products WHERE product_id = prod_id),
                    'quantity', qty,
                    'price', prod_price,
                    'discount', discount
                )
            );
        END LOOP;

        -- Insert into normalized orders
        INSERT INTO orders (customer_id, billing_address_id, shipping_address_id, total_amount, order_status)
        VALUES (cust_id, billing_id, shipping_id, total, 'completed')
        RETURNING orders.order_id INTO order_id;

        -- Insert order items into normalized table
        FOR item IN SELECT * FROM jsonb_array_elements(js->'items') LOOP
            INSERT INTO order_items (order_id, product_id, quantity, price_at_time, discount_percent)
            VALUES (
                order_id,
                (item->>'product_id')::INT,
                (item->>'quantity')::INT,
                (item->>'price')::NUMERIC,
                (item->>'discount')::NUMERIC
            );
        END LOOP;

        -- Insert into JSONB orders table
        INSERT INTO orders_jsonb (order_date, order_status, total_amount, data)
        VALUES (CURRENT_TIMESTAMP, 'completed', total, js);
    END LOOP;

    RAISE NOTICE 'Inserted % orders into both normalized and JSONB tables.', sample_size;
END;
$$ LANGUAGE plpgsql;
