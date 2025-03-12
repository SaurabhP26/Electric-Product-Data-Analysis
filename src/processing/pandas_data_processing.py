#!/usr/bin/env python3
"""
Data processing script for electric product data.
This script performs data cleaning, standardization, and transformation
on the sample electric product dataset using pandas instead of PySpark
due to environment constraints.
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime

def load_data(data_dir):
    """Load data from CSV files."""
    products_df = pd.read_csv(f"{data_dir}/products.csv")
    customers_df = pd.read_csv(f"{data_dir}/customers.csv")
    stores_df = pd.read_csv(f"{data_dir}/stores.csv")
    sales_df = pd.read_csv(f"{data_dir}/sales.csv")
    performance_df = pd.read_csv(f"{data_dir}/product_performance.csv")
    
    print(f"Loaded data from {data_dir}")
    print(f"Products: {len(products_df)} records")
    print(f"Customers: {len(customers_df)} records")
    print(f"Stores: {len(stores_df)} records")
    print(f"Sales: {len(sales_df)} records")
    print(f"Performance: {len(performance_df)} records")
    
    return products_df, customers_df, stores_df, sales_df, performance_df

def clean_products_data(df):
    """Clean and standardize products data."""
    # Handle missing values
    df['warranty_years'] = df['warranty_years'].fillna(1)
    df['weight_kg'] = df['weight_kg'].fillna(0.0)
    df['stock_quantity'] = df['stock_quantity'].fillna(0)
    
    # Standardize text fields
    df['product_name'] = df['product_name'].str.strip()
    df['category'] = df['category'].str.upper()
    df['subcategory'] = df['subcategory'].str.strip()
    
    # Convert data types
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['manufacturing_cost'] = pd.to_numeric(df['manufacturing_cost'], errors='coerce')
    df['weight_kg'] = pd.to_numeric(df['weight_kg'], errors='coerce')
    df['stock_quantity'] = pd.to_numeric(df['stock_quantity'], errors='coerce').astype('Int64')
    
    # Add derived columns
    df['profit_margin'] = round((df['price'] - df['manufacturing_cost']) / df['price'] * 100, 2)
    
    # Convert date strings to date type
    df['launch_date'] = pd.to_datetime(df['launch_date'], errors='coerce')
    
    print("Products data cleaned and standardized")
    return df

def clean_customers_data(df):
    """Clean and standardize customers data."""
    # Handle missing values
    df['lifetime_value'] = df['lifetime_value'].fillna(0.0)
    
    # Standardize text fields
    df['customer_name'] = df['customer_name'].str.strip()
    df['country'] = df['country'].str.upper()
    df['segment'] = df['segment'].str.strip()
    
    # Convert data types
    df['lifetime_value'] = pd.to_numeric(df['lifetime_value'], errors='coerce')
    
    # Convert date strings to date type
    df['acquisition_date'] = pd.to_datetime(df['acquisition_date'], errors='coerce')
    
    # Add derived columns
    df['acquisition_year'] = df['acquisition_date'].dt.year
    df['acquisition_month'] = df['acquisition_date'].dt.month
    
    print("Customers data cleaned and standardized")
    return df

def clean_stores_data(df):
    """Clean and standardize stores data."""
    # Handle missing values
    df['size_sqm'] = df['size_sqm'].fillna(0)
    
    # Standardize text fields
    df['store_name'] = df['store_name'].str.strip()
    df['country'] = df['country'].str.upper()
    df['store_type'] = df['store_type'].str.strip()
    
    # Convert data types
    df['size_sqm'] = pd.to_numeric(df['size_sqm'], errors='coerce').astype('Int64')
    
    # Convert date strings to date type
    df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')
    
    # Add derived columns
    df['opening_year'] = df['opening_date'].dt.year
    
    print("Stores data cleaned and standardized")
    return df

def clean_sales_data(df):
    """Clean and standardize sales data."""
    # Handle missing values
    df['quantity'] = df['quantity'].fillna(1)
    df['discount'] = df['discount'].fillna(0.0)
    
    # Convert data types
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').astype('Int64')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['discount'] = pd.to_numeric(df['discount'], errors='coerce')
    df['final_price'] = pd.to_numeric(df['final_price'], errors='coerce')
    df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')
    
    # Standardize text fields
    df['payment_method'] = df['payment_method'].str.strip()
    
    # Convert date strings to date type
    df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
    
    # Add derived columns
    df['transaction_year'] = df['transaction_date'].dt.year
    df['transaction_month'] = df['transaction_date'].dt.month
    df['transaction_day'] = df['transaction_date'].dt.day
    
    print("Sales data cleaned and standardized")
    return df

def clean_performance_data(df):
    """Clean and standardize product performance data."""
    # Handle missing values
    df['energy_consumption_kwh'] = df['energy_consumption_kwh'].fillna(0.0)
    df['failure_rate'] = df['failure_rate'].fillna(0.0)
    df['customer_satisfaction'] = df['customer_satisfaction'].fillna(3.0)
    df['return_rate'] = df['return_rate'].fillna(0.0)
    df['warranty_claims'] = df['warranty_claims'].fillna(0)
    
    # Convert data types
    df['energy_consumption_kwh'] = pd.to_numeric(df['energy_consumption_kwh'], errors='coerce')
    df['failure_rate'] = pd.to_numeric(df['failure_rate'], errors='coerce')
    df['customer_satisfaction'] = pd.to_numeric(df['customer_satisfaction'], errors='coerce')
    df['return_rate'] = pd.to_numeric(df['return_rate'], errors='coerce')
    df['warranty_claims'] = pd.to_numeric(df['warranty_claims'], errors='coerce').astype('Int64')
    
    # Extract year and month from year_month field
    df['year'] = df['year_month'].str[:4].astype(int)
    df['month'] = df['year_month'].str[5:7].astype(int)
    
    print("Performance data cleaned and standardized")
    return df

def save_processed_data(dfs, output_dir):
    """Save processed dataframes to CSV files."""
    products_df, customers_df, stores_df, sales_df, performance_df = dfs
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Save dataframes to CSV files
    products_df.to_csv(f"{output_dir}/products_processed.csv", index=False)
    customers_df.to_csv(f"{output_dir}/customers_processed.csv", index=False)
    stores_df.to_csv(f"{output_dir}/stores_processed.csv", index=False)
    sales_df.to_csv(f"{output_dir}/sales_processed.csv", index=False)
    performance_df.to_csv(f"{output_dir}/performance_processed.csv", index=False)
    
    print(f"Processed data saved to {output_dir}")

def main():
    """Main function to process electric product data."""
    print("Starting electric product data processing...")
    
    # Define input and output directories
    input_dir = "src/data/sample_data"
    output_dir = "src/data/processed_data"
    
    # Load data
    products_df, customers_df, stores_df, sales_df, performance_df = load_data(input_dir)
    
    # Clean and standardize data
    products_df_clean = clean_products_data(products_df)
    customers_df_clean = clean_customers_data(customers_df)
    stores_df_clean = clean_stores_data(stores_df)
    sales_df_clean = clean_sales_data(sales_df)
    performance_df_clean = clean_performance_data(performance_df)
    
    # Save processed data
    save_processed_data(
        (products_df_clean, customers_df_clean, stores_df_clean, sales_df_clean, performance_df_clean),
        output_dir
    )
    
    # Show sample of processed data
    print("\nSample of processed products data:")
    print(products_df_clean.head(5))
    
    print("\nSample of processed sales data:")
    print(sales_df_clean.head(5))
    
    print("Electric product data processing completed successfully!")

if __name__ == "__main__":
    main()
