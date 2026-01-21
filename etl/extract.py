"""
ETL Step 1: Extract
===================
This module reads raw CSV files from the data/raw directory.
It uses pandas for efficient CSV reading and data handling.
"""

import pandas as pd
from pathlib import Path

# Get the project root directory (parent of etl folder)
PROJECT_ROOT = Path(__file__).parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


def extract_sales_data():
    """
    Extract sales data from CSV file.
    
    Returns:
        pandas.DataFrame: Sales data with columns:
            - sale_id, sale_date, customer_id, product_id,
              quantity, unit_price, total_amount
    """
    file_path = RAW_DATA_DIR / "sales.csv"
    
    if not file_path.exists():
        raise FileNotFoundError(f"Sales file not found: {file_path}")
    
    # Read CSV file
    df = pd.read_csv(file_path)
    
    print(f"✓ Extracted {len(df)} sales records from sales.csv")
    return df


def extract_customers_data():
    """
    Extract customer data from CSV file.
    
    Returns:
        pandas.DataFrame: Customer data with columns:
            - customer_id, customer_name, email, city, country
    """
    file_path = RAW_DATA_DIR / "customers.csv"
    
    if not file_path.exists():
        raise FileNotFoundError(f"Customers file not found: {file_path}")
    
    # Read CSV file
    df = pd.read_csv(file_path)
    
    print(f"✓ Extracted {len(df)} customer records from customers.csv")
    return df


def extract_products_data():
    """
    Extract product data from CSV file.
    
    Returns:
        pandas.DataFrame: Product data with columns:
            - product_id, product_name, category, subcategory, unit_cost
    """
    file_path = RAW_DATA_DIR / "products.csv"
    
    if not file_path.exists():
        raise FileNotFoundError(f"Products file not found: {file_path}")
    
    # Read CSV file
    df = pd.read_csv(file_path)
    
    print(f"✓ Extracted {len(df)} product records from products.csv")
    return df


def extract_all():
    """
    Extract all data files at once.
    
    Returns:
        tuple: (sales_df, customers_df, products_df)
    """
    print("=" * 50)
    print("EXTRACTING DATA FROM CSV FILES")
    print("=" * 50)
    
    sales_df = extract_sales_data()
    customers_df = extract_customers_data()
    products_df = extract_products_data()
    
    print("=" * 50)
    print("EXTRACTION COMPLETE")
    print("=" * 50)
    
    return sales_df, customers_df, products_df


if __name__ == "__main__":
    # Test extraction
    sales, customers, products = extract_all()
    
    print("\nSample Sales Data:")
    print(sales.head())
    print("\nSample Customers Data:")
    print(customers.head())
    print("\nSample Products Data:")
    print(products.head())
