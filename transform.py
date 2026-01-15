"""
ETL Step 2: Transform
=====================
This module cleans and transforms the extracted data.
It handles data quality issues and creates the date dimension.
"""

import pandas as pd
from datetime import datetime


def remove_duplicates(df, subset=None):
    """
    Remove duplicate rows from dataframe.
    
    Args:
        df: pandas DataFrame
        subset: list of columns to check for duplicates (None = all columns)
    
    Returns:
        pandas.DataFrame: Dataframe without duplicates
    """
    initial_count = len(df)
    df_cleaned = df.drop_duplicates(subset=subset)
    removed = initial_count - len(df_cleaned)
    
    if removed > 0:
        print(f"  → Removed {removed} duplicate rows")
    
    return df_cleaned


def handle_missing_values(df, strategy='drop'):
    """
    Handle missing values in dataframe.
    
    Args:
        df: pandas DataFrame
        strategy: 'drop' to remove rows with missing values,
                  'fill' to fill with default values
    
    Returns:
        pandas.DataFrame: Dataframe with handled missing values
    """
    initial_count = len(df)
    
    if strategy == 'drop':
        df_cleaned = df.dropna()
        removed = initial_count - len(df_cleaned)
        if removed > 0:
            print(f"  → Removed {removed} rows with missing values")
    else:
        # Fill numeric columns with 0, string columns with 'Unknown'
        df_cleaned = df.copy()
        for col in df_cleaned.columns:
            if df_cleaned[col].dtype in ['int64', 'float64']:
                df_cleaned[col].fillna(0, inplace=True)
            else:
                df_cleaned[col].fillna('Unknown', inplace=True)
        print(f"  → Filled missing values")
    
    return df_cleaned


def transform_sales_data(sales_df):
    """
    Transform sales data: clean and validate.
    
    Args:
        sales_df: Raw sales dataframe
    
    Returns:
        pandas.DataFrame: Cleaned sales dataframe
    """
    print("\nTransforming Sales Data...")
    df = sales_df.copy()
    
    # Remove duplicates based on sale_id
    df = remove_duplicates(df, subset=['sale_id'])
    
    # Handle missing values
    df = handle_missing_values(df, strategy='drop')
    
    # Convert sale_date to datetime
    df['sale_date'] = pd.to_datetime(df['sale_date'])
    
    # Ensure numeric columns are correct type
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')
    
    # Remove rows where critical numeric fields are invalid
    df = df.dropna(subset=['quantity', 'unit_price', 'total_amount'])
    
    # Ensure quantity and prices are positive
    df = df[(df['quantity'] > 0) & (df['unit_price'] > 0) & (df['total_amount'] > 0)]
    
    print(f"✓ Transformed {len(df)} sales records")
    return df


def transform_customers_data(customers_df):
    """
    Transform customer data: clean and validate.
    
    Args:
        customers_df: Raw customers dataframe
    
    Returns:
        pandas.DataFrame: Cleaned customers dataframe
    """
    print("\nTransforming Customers Data...")
    df = customers_df.copy()
    
    # Remove duplicates based on customer_id
    df = remove_duplicates(df, subset=['customer_id'])
    
    # Handle missing values
    df = handle_missing_values(df, strategy='drop')
    
    # Ensure customer_id is integer
    df['customer_id'] = pd.to_numeric(df['customer_id'], errors='coerce')
    df = df.dropna(subset=['customer_id'])
    df['customer_id'] = df['customer_id'].astype(int)
    
    print(f"✓ Transformed {len(df)} customer records")
    return df


def transform_products_data(products_df):
    """
    Transform product data: clean and validate.
    
    Args:
        products_df: Raw products dataframe
    
    Returns:
        pandas.DataFrame: Cleaned products dataframe
    """
    print("\nTransforming Products Data...")
    df = products_df.copy()
    
    # Remove duplicates based on product_id
    df = remove_duplicates(df, subset=['product_id'])
    
    # Handle missing values
    df = handle_missing_values(df, strategy='drop')
    
    # Ensure product_id is integer
    df['product_id'] = pd.to_numeric(df['product_id'], errors='coerce')
    df = df.dropna(subset=['product_id'])
    df['product_id'] = df['product_id'].astype(int)
    
    # Ensure unit_cost is numeric
    df['unit_cost'] = pd.to_numeric(df['unit_cost'], errors='coerce')
    df['unit_cost'] = df['unit_cost'].fillna(0)
    
    print(f"✓ Transformed {len(df)} product records")
    return df


def create_date_dimension(sales_df):
    """
    Create date dimension table from unique dates in sales data.
    This is a common pattern in data warehousing.
    
    Args:
        sales_df: Sales dataframe with 'sale_date' column
    
    Returns:
        pandas.DataFrame: Date dimension with all date attributes
    """
    print("\nCreating Date Dimension...")
    
    # Get unique dates from sales data
    unique_dates = sales_df['sale_date'].unique()
    
    date_records = []
    
    for date in unique_dates:
        # Extract date components
        date_obj = pd.to_datetime(date)
        
        day = date_obj.day
        month = date_obj.month
        quarter = (month - 1) // 3 + 1
        year = date_obj.year
        
        # Month name
        month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                      'July', 'August', 'September', 'October', 'November', 'December']
        month_name = month_names[month - 1]
        
        # Quarter name
        quarter_name = f"Q{quarter}"
        
        # Day of week
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        day_of_week = day_names[date_obj.weekday()]
        
        # Check if weekend
        is_weekend = date_obj.weekday() >= 5
        
        date_records.append({
            'sale_date': date_obj.date(),
            'day': day,
            'month': month,
            'quarter': quarter,
            'year': year,
            'month_name': month_name,
            'quarter_name': quarter_name,
            'day_of_week': day_of_week,
            'is_weekend': is_weekend
        })
    
    date_dim = pd.DataFrame(date_records)
    
    # Sort by date
    date_dim = date_dim.sort_values('sale_date').reset_index(drop=True)
    
    print(f"✓ Created date dimension with {len(date_dim)} unique dates")
    return date_dim


def transform_all(sales_df, customers_df, products_df):
    """
    Transform all dataframes at once.
    
    Args:
        sales_df: Raw sales dataframe
        customers_df: Raw customers dataframe
        products_df: Raw products dataframe
    
    Returns:
        tuple: (transformed_sales, transformed_customers, 
                transformed_products, date_dimension)
    """
    print("=" * 50)
    print("TRANSFORMING DATA")
    print("=" * 50)
    
    sales_clean = transform_sales_data(sales_df)
    customers_clean = transform_customers_data(customers_df)
    products_clean = transform_products_data(products_df)
    date_dim = create_date_dimension(sales_clean)
    
    print("=" * 50)
    print("TRANSFORMATION COMPLETE")
    print("=" * 50)
    
    return sales_clean, customers_clean, products_clean, date_dim


if __name__ == "__main__":
    # Test transformation (requires extract module)
    from extract import extract_all
    
    sales, customers, products = extract_all()
    sales_t, customers_t, products_t, date_dim = transform_all(sales, customers, products)
    
    print("\nSample Transformed Sales Data:")
    print(sales_t.head())
    print("\nSample Date Dimension:")
    print(date_dim.head())
