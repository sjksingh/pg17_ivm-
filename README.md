# pg_ivm Real-Time Materialized Views Demo

This repository demonstrates **pg_ivm** (PostgreSQL Incremental View Maintenance) - an extension that creates materialized views that automatically stay synchronized with their base tables.

## 🚀 Quick Start with Docker

### Prerequisites
- Docker installed on your system

### Get Started 

```bash
# Clone this repository
git clone https://github.com/sjksingh/pg17_ivm-.git
cd pg17_ivm-

# Run PostgreSQL 17 with pg_ivm pre-installed
bash build.sh

# Connect to the database
docker exec -it pg17-extended psql -U postgres -d partitioning_test
```

## 📦 What's Inside

This Docker image includes PostgreSQL 17 with these extensions:

| Extension | Version | Purpose |
|-----------|---------|---------|
| **pg_ivm** | 1.11 | **Incremental View Maintenance** |
| citus | 13.1-1 | Distributed PostgreSQL |
| vector | 0.8.0 | AI/ML vector operations |
| pg_partman | 5.2.4 | Partition management |
| pg_cron | 1.6 | Job scheduling |
| pg_stat_statements | 1.11 | Query performance tracking |

## 🎯 The Demo: E-commerce Analytics

### Problem Statement
Traditional materialized views require manual `REFRESH` operations:
- `REFRESH MATERIALIZED VIEW` - Locks the view during full recompute
- `REFRESH MATERIALIZED VIEW CONCURRENTLY` - Allows queries but still recomputes everything

**pg_ivm solves this by updating only the changed data automatically.**

### Step-by-Step Demo

#### 1. Setup Environment
```sql
-- Create demo schema
CREATE SCHEMA ivm;
SET search_path = ivm;
```

#### 2. Create Base Tables
```sql
-- Products catalog
CREATE TABLE products (
  product_id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  category TEXT NOT NULL,
  price NUMERIC NOT NULL
);

-- Orders table
CREATE TABLE orders (
  order_id SERIAL PRIMARY KEY,
  product_id INT REFERENCES products(product_id),
  quantity INT NOT NULL,
  order_time TIMESTAMP DEFAULT now()
);
```

#### 3. Load Sample Data
```sql
-- Insert sample products
INSERT INTO products (name, category, price) VALUES
  ('iPhone', 'Electronics', 999),
  ('MacBook', 'Electronics', 1999),
  ('iPad', 'Electronics', 599),
  ('AirPods', 'Electronics', 199),
  ('T-shirt', 'Apparel', 20),
  ('Jeans', 'Apparel', 80),
  ('Sneakers', 'Apparel', 60),
  ('Jacket', 'Apparel', 120),
  ('Coffee Mug', 'Home', 15),
  ('Desk Lamp', 'Home', 45),
  ('Pillow', 'Home', 25),
  ('Blanket', 'Home', 35);
```

#### 4. Create Helper Function
```sql
-- Function to generate random test orders
CREATE OR REPLACE FUNCTION add_random_orders(num_orders INT DEFAULT 1)
RETURNS INT AS $$
DECLARE
    i INT;
    random_product_id INT;
    random_quantity INT;
    max_product_id INT;
    orders_created INT := 0;
BEGIN
    SELECT MAX(p.product_id) INTO max_product_id FROM products p;
    
    FOR i IN 1..num_orders LOOP
        random_product_id := floor(random() * max_product_id + 1)::INT;
        random_quantity := floor(random() * 10 + 1)::INT;
        
        INSERT INTO orders (product_id, quantity)
        VALUES (random_product_id, random_quantity);
        
        orders_created := orders_created + 1;
    END LOOP;
    
    RETURN orders_created;
END;
$$ LANGUAGE plpgsql;

-- Generate initial test data
SELECT add_random_orders(100);
```

#### 5. Create Incremental Materialized View
```sql
-- This is where the magic happens!
SELECT pgivm.create_immv(
  'category_sales_summary',
  $$
    SELECT
      p.category,
      COUNT(*) AS num_orders,
      SUM(o.quantity) AS total_quantity,
      SUM(o.quantity * p.price) AS total_revenue
    FROM orders o
    JOIN products p ON o.product_id = p.product_id
    GROUP BY p.category
  $$
);
```

#### 6. Test Real-Time Updates
```sql
-- Check current state
SELECT 'BEFORE:' as status, category, num_orders, total_revenue 
FROM category_sales_summary ORDER BY category;

-- Add a high-value order
INSERT INTO orders (product_id, quantity) VALUES (2, 5); -- 5 MacBooks

-- Check immediately - NO REFRESH NEEDED!
SELECT 'AFTER:' as status, category, num_orders, total_revenue 
FROM category_sales_summary ORDER BY category;
```

**Expected Result:**
```
 status |  category   | num_orders | total_revenue 
--------+-------------+------------+---------------
 BEFORE | Electronics |         35 |       534891
 AFTER  | Electronics |         36 |       544886  -- Updated instantly!
```

## 🔍 Understanding pg_ivm Internals

### How It Works
1. **Trigger Installation** - Automatically creates triggers on base tables
2. **Change Capture** - Monitors INSERT/UPDATE/DELETE operations  
3. **Delta Calculation** - Computes only the impact of changes
4. **Incremental Updates** - Applies minimal updates to the view

### View the Triggers
```sql
-- See the triggers pg_ivm created
SELECT tgname, tgrelid::regclass::text AS table_name
FROM pg_trigger
WHERE tgname ILIKE '%ivm%';
```

### Check Sizes
```sql
-- Compare object sizes
SELECT 
    'orders' as object_name,
    'Table' as type,
    pg_size_pretty(pg_total_relation_size('orders')) AS size
UNION ALL
SELECT 
    'category_sales_summary' as object_name,
    'Incremental Materialized View' as type,
    pg_size_pretty(pg_total_relation_size('category_sales_summary')) AS size
ORDER BY object_name;
```

## 🎯 Use Cases

### ✅ Perfect For:
- **Real-time dashboards** requiring fresh data
- **Category summaries** and GROUP BY aggregations  
- **Live leaderboards** and ranking systems
- **OLTP analytics** with frequent aggregate queries
- **Event-driven architectures** needing immediate consistency

### ⚠️ Consider Alternatives For:
- **Very large datasets** (row-by-row processing overhead)
- **Complex queries** with window functions
- **High-volume writes** (trigger overhead)
- **Batch processing** where staleness is acceptable

## 📊 Performance Testing

### Generate Load
```sql
-- Add more test data
SELECT add_random_orders(1000);

-- Monitor performance
SELECT category, num_orders, total_revenue 
FROM category_sales_summary 
ORDER BY total_revenue DESC;
```

### Compare with Regular Materialized Views
```sql
-- Create regular materialized view for comparison
CREATE MATERIALIZED VIEW regular_category_summary AS
SELECT
    p.category,
    COUNT(*) AS num_orders,
    SUM(o.quantity) AS total_quantity,
    SUM(o.quantity * p.price) AS total_revenue
FROM orders o
JOIN products p ON o.product_id = p.product_id
GROUP BY p.category;

-- Add more orders
SELECT add_random_orders(50);

-- Compare results
SELECT 'IVM (auto-updated)' as type, category, num_orders 
FROM category_sales_summary ORDER BY category
UNION ALL
SELECT 'Regular MV (stale)' as type, category, num_orders 
FROM regular_category_summary ORDER BY category;
```

## 🛠️ Management Commands

### View IMMV Definition
```sql
SELECT pgivm.get_immv_def('category_sales_summary');
```

### Drop IMMV
```sql
SELECT pgivm.drop_immv('category_sales_summary');
```

### Clean View for Applications
```sql
-- Hide internal columns from applications
CREATE VIEW category_sales_clean AS
SELECT category, num_orders, total_quantity, total_revenue
FROM category_sales_summary;

SELECT * FROM category_sales_clean;
```

## 🐳 Docker Environment Details

### Container Specs
- **Base**: PostgreSQL 17
- **Extensions**: 12+ pre-installed including pg_ivm 1.11
- **Purpose**: Development, testing, and demonstration

### Environment Variables
```bash
POSTGRES_PASSWORD=demo123  # Default password
POSTGRES_DB=postgres       # Default database
POSTGRES_USER=postgres     # Default user
```

📉 Limitations of pg_ivm

While pg_ivm is powerful, it’s not always the best fit. Here are its key limitations and trade-offs:

⚠️ Limitations
	•	Limited SQL Coverage:
Only supports a subset of SELECT queries—GROUP BY, joins, aggregates. No DISTINCT, HAVING, or window functions.
	•	Trigger Overhead:
Each base table update fires triggers, which may introduce latency or contention in write-heavy systems.
	•	Row-by-Row Delta Maintenance:
Unlike full-refresh, it updates per row change, which can become inefficient for high-churn workloads.
	•	No Automatic Schema Change Handling:
Changes in base tables (like renaming a column) require dropping and recreating the IMMV manually.
	•	Debuggability:
Debugging IMMV logic may be harder due to generated triggers and internal metadata.

### Storage vs Performance Tradeoff


| Approach                    | Storage          | Query Speed                       | Freshness              | Complexity        |
|-----------------------------|------------------|-----------------------------------|------------------------|-------------------|
| **Regular View**            | 🟢 None          | 🔴 Slow (recomputed every time)  | 🟢 Always fresh        | 🟢 Very low       |
| **Materialized View (MV)**  | 🔴 High          | 🟢 Fast (precomputed results)    | 🔴 Stale until refresh | 🟢 Low            |
| **pg_ivm (Incremental MV)** | 🟡 Moderate      | 🟢 Fast (auto-updated deltas)    | 🟢 Near real-time      | 🟠 Medium         |



## 📚 Additional Resources

- **pg_ivm GitHub**: https://github.com/sraoss/pg_ivm
- **PostgreSQL Documentation**: https://www.postgresql.org/docs/
- **Blog Post**: [Detailed explanation and use cases]



## 📄 License

This demo repository is provided for educational purposes. Please check individual extension licenses for usage terms.

---

**Happy querying with real-time materialized views! 🚀**
