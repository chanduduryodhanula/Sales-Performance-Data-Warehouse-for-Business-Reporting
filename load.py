"""
ETL Step 3: Load
================
This module loads transformed data into PostgreSQL database.
It uses transactions to ensure data integrity.
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import pandas as pd
from pathlib import Path

# Database connection parameters
# Update these according to your PostgreSQL setup
DB_CONFIG = {
    'host': 'localhost',
    'database': 'data_warehouse',
    'user': 'postgres',
    'password': 'postgres',
    'port': 5432
}


def get_connection():
    """
    Create and return a database connection.
    
    Returns:
        psycopg2.connection: Database connection object
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")
        raise


def execute_sql_file(conn, file_path):
    """
    Execute SQL commands from a file.
    
    Args:
        conn: Database connection
        file_path: Path to SQL file
    """
    with open(file_path, 'r') as f:
        sql_commands = f.read()
    
    cursor = conn.cursor()
    try:
        cursor.execute(sql_commands)
        conn.commit()
        print(f"✓ Executed SQL file: {file_path}")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error executing SQL file: {e}")
        raise
    finally:
        cursor.close()


def load_dim_customer(conn, customers_df):
    """
    Load customer dimension data.
    
    Args:
        conn: Database connection
        customers_df: Transformed customers dataframe
    """
    print("\nLoading Customer Dimension...")
    
    cursor = conn.cursor()
    
    try:
        # Prepare data for insertion
        records = customers_df.to_dict('records')
        
        insert_query = """
            INSERT INTO dim_customer (customer_id, customer_name, email, city, country)
            VALUES %s
            ON CONFLICT (customer_id) DO UPDATE SET
                customer_name = EXCLUDED.customer_name,
                email = EXCLUDED.email,
                city = EXCLUDED.city,
                country = EXCLUDED.country,
                updated_at = CURRENT_TIMESTAMP
        """
        
        values = [(r['customer_id'], r['customer_name'], r['email'], 
                  r['city'], r['country']) for r in records]
        
        execute_values(cursor, insert_query, values)
        conn.commit()
        
        print(f"✓ Loaded {len(records)} customer records")
        
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error loading customer dimension: {e}")
        raise
    finally:
        cursor.close()


def load_dim_product(conn, products_df):
    """
    Load product dimension data.
    
    Args:
        conn: Database connection
        products_df: Transformed products dataframe
    """
    print("\nLoading Product Dimension...")
    
    cursor = conn.cursor()
    
    try:
        # Prepare data for insertion
        records = products_df.to_dict('records')
        
        insert_query = """
            INSERT INTO dim_product (product_id, product_name, category, subcategory, unit_cost)
            VALUES %s
            ON CONFLICT (product_id) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                category = EXCLUDED.category,
                subcategory = EXCLUDED.subcategory,
                unit_cost = EXCLUDED.unit_cost,
                updated_at = CURRENT_TIMESTAMP
        """
        
        values = [(r['product_id'], r['product_name'], r['category'], 
                  r['subcategory'], r['unit_cost']) for r in records]
        
        execute_values(cursor, insert_query, values)
        conn.commit()
        
        print(f"✓ Loaded {len(records)} product records")
        
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error loading product dimension: {e}")
        raise
    finally:
        cursor.close()


def load_dim_date(conn, date_df):
    """
    Load date dimension data.
    
    Args:
        conn: Database connection
        date_df: Date dimension dataframe
    """
    print("\nLoading Date Dimension...")
    
    cursor = conn.cursor()
    
    try:
        # Prepare data for insertion
        records = date_df.to_dict('records')
        
        insert_query = """
            INSERT INTO dim_date (sale_date, day, month, quarter, year, 
                                 month_name, quarter_name, day_of_week, is_weekend)
            VALUES %s
            ON CONFLICT (sale_date) DO NOTHING
        """
        
        values = [(r['sale_date'], r['day'], r['month'], r['quarter'], 
                  r['year'], r['month_name'], r['quarter_name'], 
                  r['day_of_week'], r['is_weekend']) for r in records]
        
        execute_values(cursor, insert_query, values)
        conn.commit()
        
        print(f"✓ Loaded {len(records)} date records")
        
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error loading date dimension: {e}")
        raise
    finally:
        cursor.close()


def load_fact_sales(conn, sales_df):
    """
    Load sales fact data.
    This requires lookups to dimension tables to get surrogate keys.
    
    Args:
        conn: Database connection
        sales_df: Transformed sales dataframe
    """
    print("\nLoading Sales Fact Table...")
    
    cursor = conn.cursor()
    
    try:
        # First, get all dimension keys for lookups
        # Get date keys
        date_lookup = {}
        cursor.execute("SELECT date_key, sale_date FROM dim_date")
        for row in cursor.fetchall():
            date_lookup[pd.to_datetime(row[1]).date()] = row[0]
        
        # Get customer keys
        customer_lookup = {}
        cursor.execute("SELECT customer_key, customer_id FROM dim_customer")
        for row in cursor.fetchall():
            customer_lookup[row[1]] = row[0]
        
        # Get product keys
        product_lookup = {}
        cursor.execute("SELECT product_key, product_id FROM dim_product")
        for row in cursor.fetchall():
            product_lookup[row[1]] = row[0]
        
        # Prepare fact records with surrogate keys
        fact_records = []
        skipped = 0
        
        for _, row in sales_df.iterrows():
            sale_date = pd.to_datetime(row['sale_date']).date()
            customer_id = int(row['customer_id'])
            product_id = int(row['product_id'])
            
            # Lookup surrogate keys
            date_key = date_lookup.get(sale_date)
            customer_key = customer_lookup.get(customer_id)
            product_key = product_lookup.get(product_id)
            
            # Skip if any lookup fails (data quality issue)
            if not all([date_key, customer_key, product_key]):
                skipped += 1
                continue
            
            fact_records.append((
                int(row['sale_id']),
                date_key,
                customer_key,
                product_key,
                int(row['quantity']),
                float(row['unit_price']),
                float(row['total_amount'])
            ))
        
        if skipped > 0:
            print(f"  → Skipped {skipped} records due to missing dimension keys")
        
        # Insert fact records
        insert_query = """
            INSERT INTO fact_sales (sale_id, date_key, customer_key, product_key, 
                                   quantity, unit_price, total_amount)
            VALUES %s
            ON CONFLICT (sale_id) DO UPDATE SET
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                total_amount = EXCLUDED.total_amount
        """
        
        execute_values(cursor, insert_query, fact_records)
        conn.commit()
        
        print(f"✓ Loaded {len(fact_records)} sales fact records")
        
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error loading sales fact table: {e}")
        raise
    finally:
        cursor.close()


def load_all(sales_df, customers_df, products_df, date_df):
    """
    Load all data into the data warehouse.
    This is the main load function that orchestrates the entire load process.
    
    Args:
        sales_df: Transformed sales dataframe
        customers_df: Transformed customers dataframe
        products_df: Transformed products dataframe
        date_df: Date dimension dataframe
    """
    print("=" * 50)
    print("LOADING DATA INTO DATA WAREHOUSE")
    print("=" * 50)
    
    conn = None
    
    try:
        # Connect to database
        conn = get_connection()
        print("✓ Connected to PostgreSQL database")
        
        # Create tables (if not exists, this will be handled by create_tables.sql)
        # For now, we assume tables are already created
        # You can uncomment the following to create tables automatically:
        # project_root = Path(__file__).parent.parent
        # sql_file = project_root / "sql" / "create_tables.sql"
        # execute_sql_file(conn, sql_file)
        
        # Load dimensions first (required for fact table foreign keys)
        load_dim_customer(conn, customers_df)
        load_dim_product(conn, products_df)
        load_dim_date(conn, date_df)
        
        # Load fact table last (depends on dimensions)
        load_fact_sales(conn, sales_df)
        
        print("=" * 50)
        print("DATA LOADING COMPLETE")
        print("=" * 50)
        
    except Exception as e:
        print(f"Error during data loading: {e}")
        raise
    finally:
        if conn:
            conn.close()
            print("✓ Database connection closed")


if __name__ == "__main__":
    # Test loading (requires extract and transform modules)
    from extract import extract_all
    from transform import transform_all
    
    print("Starting ETL Pipeline...")
    
    # Extract
    sales, customers, products = extract_all()
    
    # Transform
    sales_t, customers_t, products_t, date_dim = transform_all(sales, customers, products)
    
    # Load
    load_all(sales_t, customers_t, products_t, date_dim)
    
    print("\nETL Pipeline completed successfully!")
