"""
ETL Pipeline Runner
===================
This script runs the complete ETL pipeline:
1. Extract data from CSV files
2. Transform and clean the data
3. Load data into PostgreSQL data warehouse
"""

from etl.extract import extract_all
from etl.transform import transform_all
from etl.load import load_all


def main():
    """
    Main function to run the complete ETL pipeline.
    """
    print("=" * 60)
    print("DATA WAREHOUSE ETL PIPELINE")
    print("=" * 60)
    print()
    
    try:
        # Step 1: Extract
        print("STEP 1: EXTRACTING DATA FROM CSV FILES")
        print("-" * 60)
        sales, customers, products = extract_all()
        print()
        
        # Step 2: Transform
        print("STEP 2: TRANSFORMING DATA")
        print("-" * 60)
        sales_t, customers_t, products_t, date_dim = transform_all(
            sales, customers, products
        )
        print()
        
        # Step 3: Load
        print("STEP 3: LOADING DATA INTO DATA WAREHOUSE")
        print("-" * 60)
        load_all(sales_t, customers_t, products_t, date_dim)
        print()
        
        print("=" * 60)
        print("ETL PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print()
        print("Next steps:")
        print("1. Run analytics queries from sql/analytics_queries.sql")
        print("2. Connect to PostgreSQL and explore the data warehouse")
        
    except Exception as e:
        print()
        print("=" * 60)
        print("ERROR: ETL PIPELINE FAILED")
        print("=" * 60)
        print(f"Error message: {str(e)}")
        print()
        print("Troubleshooting:")
        print("1. Check PostgreSQL is running")
        print("2. Verify database credentials in etl/load.py")
        print("3. Ensure tables are created (run sql/create_tables.sql)")
        raise


if __name__ == "__main__":
    main()
