-- ============================================
-- Analytics Queries for Data Warehouse
-- ============================================
-- These queries demonstrate how the star schema
-- enables fast and intuitive analytics

-- ============================================
-- 1. MONTHLY SALES REPORT
-- ============================================
-- Shows total sales amount and quantity by month
SELECT 
    d.year,
    d.month,
    d.month_name,
    COUNT(f.sale_id) AS total_transactions,
    SUM(f.quantity) AS total_quantity_sold,
    SUM(f.total_amount) AS total_sales_amount,
    AVG(f.total_amount) AS avg_sale_amount
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- ============================================
-- 2. TOP PRODUCTS BY SALES
-- ============================================
-- Shows best-selling products by total revenue
SELECT 
    p.product_name,
    p.category,
    p.subcategory,
    COUNT(f.sale_id) AS total_sales,
    SUM(f.quantity) AS total_quantity_sold,
    SUM(f.total_amount) AS total_revenue,
    AVG(f.unit_price) AS avg_unit_price
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_key, p.product_name, p.category, p.subcategory
ORDER BY total_revenue DESC
LIMIT 10;

-- ============================================
-- 3. TOP CUSTOMERS BY PURCHASE VALUE
-- ============================================
-- Shows customers who spent the most
SELECT 
    c.customer_name,
    c.city,
    c.country,
    COUNT(f.sale_id) AS total_purchases,
    SUM(f.quantity) AS total_items_purchased,
    SUM(f.total_amount) AS total_spent,
    AVG(f.total_amount) AS avg_purchase_value
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_key, c.customer_name, c.city, c.country
ORDER BY total_spent DESC
LIMIT 10;

-- ============================================
-- 4. SALES BY PRODUCT CATEGORY
-- ============================================
-- Shows sales breakdown by product category
SELECT 
    p.category,
    COUNT(f.sale_id) AS total_transactions,
    SUM(f.quantity) AS total_quantity,
    SUM(f.total_amount) AS total_revenue,
    AVG(f.total_amount) AS avg_transaction_value
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;

-- ============================================
-- 5. QUARTERLY SALES SUMMARY
-- ============================================
-- Shows sales performance by quarter
SELECT 
    d.year,
    d.quarter,
    d.quarter_name,
    COUNT(f.sale_id) AS total_transactions,
    SUM(f.total_amount) AS total_revenue,
    AVG(f.total_amount) AS avg_transaction_value
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.quarter, d.quarter_name
ORDER BY d.year, d.quarter;
