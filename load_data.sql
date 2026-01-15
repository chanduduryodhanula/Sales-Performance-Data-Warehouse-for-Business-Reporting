-- ============================================
-- Data Loading Script
-- ============================================
-- This script is for reference only
-- Actual data loading is done via Python ETL scripts
-- 
-- The ETL process follows this order:
-- 1. Load dim_customer from customers.csv
-- 2. Load dim_product from products.csv
-- 3. Load dim_date (generated from sales dates)
-- 4. Load fact_sales from sales.csv (with lookups to dimensions)

-- Example manual insert (for reference):
-- INSERT INTO dim_customer (customer_id, customer_name, email, city, country)
-- VALUES (101, 'John Smith', 'john.smith@email.com', 'New York', 'USA');

-- Note: Use the Python ETL scripts (extract.py, transform.py, load.py)
-- to load data automatically from CSV files.
