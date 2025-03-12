#!/usr/bin/env python3
"""
Star Schema Data Model Implementation for Electric Product Data Analysis.
This script transforms the processed data into a star schema model
with fact and dimension tables for optimized query performance.
"""

import pandas as pd
import os
from datetime import datetime

def load_processed_data(data_dir):
    """Load processed data from CSV files."""
    products_df = pd.read_csv(f"{data_dir}/products_processed.csv")
    customers_df = pd.read_csv(f"{data_dir}/customers_processed.csv")
    stores_df = pd.read_csv(f"{data_dir}/stores_processed.csv")
    sales_df = pd.read_csv(f"{data_dir}/sales_processed.csv")
    performance_df = pd.read_csv(f"{data_dir}/performance_processed.csv")
    
    print(f"Loaded processed data from {data_dir}")
    
    return products_df, customers_df, stores_df, sales_df, performance_df

def create_date_dimension():
    """Create date dimension table."""
    # Generate dates from 2022-01-01 to 2023-12-31
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2023, 12, 31)
    
    # Create date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Create date dimension dataframe
    date_dim = pd.DataFrame({
        'date_id': range(1, len(date_range) + 1),
        'date': date_range,
        'day': date_range.day,
        'month': date_range.month,
        'quarter': date_range.quarter,
        'year': date_range.year,
        'day_of_week': date_range.dayofweek,
        'day_name': date_range.day_name(),
        'month_name': date_range.month_name(),
        'is_weekend': date_range.dayofweek.isin([5, 6])
    })
    
    # Create date_key for joining (YYYYMMDD format)
    date_dim['date_key'] = date_dim['date'].dt.strftime('%Y%m%d').astype(int)
    
    print(f"Created date dimension with {len(date_dim)} records")
    return date_dim

def create_product_dimension(products_df):
    """Create product dimension table."""
    # Select relevant columns for product dimension
    product_dim = products_df[[
        'product_id', 'product_name', 'category', 'subcategory', 
        'energy_rating', 'price', 'manufacturing_cost', 'warranty_years',
        'weight_kg', 'profit_margin', 'launch_date'
    ]].copy()
    
    # Add surrogate key
    product_dim['product_key'] = range(1, len(product_dim) + 1)
    
    # Reorder columns to put key first
    cols = ['product_key'] + [col for col in product_dim.columns if col != 'product_key']
    product_dim = product_dim[cols]
    
    print(f"Created product dimension with {len(product_dim)} records")
    return product_dim

def create_customer_dimension(customers_df):
    """Create customer dimension table."""
    # Select relevant columns for customer dimension
    customer_dim = customers_df[[
        'customer_id', 'customer_name', 'country', 'segment',
        'acquisition_date', 'lifetime_value', 'acquisition_year', 'acquisition_month'
    ]].copy()
    
    # Add surrogate key
    customer_dim['customer_key'] = range(1, len(customer_dim) + 1)
    
    # Reorder columns to put key first
    cols = ['customer_key'] + [col for col in customer_dim.columns if col != 'customer_key']
    customer_dim = customer_dim[cols]
    
    print(f"Created customer dimension with {len(customer_dim)} records")
    return customer_dim

def create_store_dimension(stores_df):
    """Create store dimension table."""
    # Select relevant columns for store dimension
    store_dim = stores_df[[
        'store_id', 'store_name', 'country', 'store_type',
        'opening_date', 'size_sqm', 'opening_year'
    ]].copy()
    
    # Add surrogate key
    store_dim['store_key'] = range(1, len(store_dim) + 1)
    
    # Reorder columns to put key first
    cols = ['store_key'] + [col for col in store_dim.columns if col != 'store_key']
    store_dim = store_dim[cols]
    
    print(f"Created store dimension with {len(store_dim)} records")
    return store_dim

def create_sales_fact(sales_df, product_dim, customer_dim, store_dim, date_dim):
    """Create sales fact table."""
    # Start with a copy of the sales dataframe
    sales_fact = sales_df.copy()
    
    # Convert transaction_date to date_key format for joining
    sales_fact['transaction_date'] = pd.to_datetime(sales_fact['transaction_date'])
    sales_fact['date_key'] = sales_fact['transaction_date'].dt.strftime('%Y%m%d').astype(int)
    
    # Create mapping dictionaries for dimension keys
    product_key_map = dict(zip(product_dim['product_id'], product_dim['product_key']))
    customer_key_map = dict(zip(customer_dim['customer_id'], customer_dim['customer_key']))
    store_key_map = dict(zip(store_dim['store_id'], store_dim['store_key']))
    
    # Map dimension keys to fact table
    sales_fact['product_key'] = sales_fact['product_id'].map(product_key_map)
    sales_fact['customer_key'] = sales_fact['customer_id'].map(customer_key_map)
    sales_fact['store_key'] = sales_fact['store_id'].map(store_key_map)
    
    # Select only necessary columns for fact table
    sales_fact = sales_fact[[
        'transaction_id', 'date_key', 'product_key', 'customer_key', 'store_key',
        'quantity', 'unit_price', 'discount', 'final_price', 'total_amount', 'payment_method'
    ]]
    
    # Add surrogate key
    sales_fact['sales_key'] = range(1, len(sales_fact) + 1)
    
    # Reorder columns to put key first
    cols = ['sales_key'] + [col for col in sales_fact.columns if col != 'sales_key']
    sales_fact = sales_fact[cols]
    
    print(f"Created sales fact table with {len(sales_fact)} records")
    return sales_fact

def create_performance_fact(performance_df, product_dim, date_dim):
    """Create product performance fact table."""
    # Start with a copy of the performance dataframe
    performance_fact = performance_df.copy()
    
    # Create date_key from year and month
    performance_fact['date_key'] = (
        performance_fact['year'].astype(str) + 
        performance_fact['month'].astype(str).str.zfill(2) + 
        '01'  # First day of month
    ).astype(int)
    
    # Create mapping dictionary for product keys
    product_key_map = dict(zip(product_dim['product_id'], product_dim['product_key']))
    
    # Map product keys to fact table
    performance_fact['product_key'] = performance_fact['product_id'].map(product_key_map)
    
    # Select only necessary columns for fact table
    performance_fact = performance_fact[[
        'date_key', 'product_key', 'energy_consumption_kwh', 'failure_rate',
        'customer_satisfaction', 'return_rate', 'warranty_claims'
    ]]
    
    # Add surrogate key
    performance_fact['performance_key'] = range(1, len(performance_fact) + 1)
    
    # Reorder columns to put key first
    cols = ['performance_key'] + [col for col in performance_fact.columns if col != 'performance_key']
    performance_fact = performance_fact[cols]
    
    print(f"Created performance fact table with {len(performance_fact)} records")
    return performance_fact

def save_star_schema(tables, output_dir):
    """Save star schema tables to CSV files."""
    date_dim, product_dim, customer_dim, store_dim, sales_fact, performance_fact = tables
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Save dimension tables
    date_dim.to_csv(f"{output_dir}/dim_date.csv", index=False)
    product_dim.to_csv(f"{output_dir}/dim_product.csv", index=False)
    customer_dim.to_csv(f"{output_dir}/dim_customer.csv", index=False)
    store_dim.to_csv(f"{output_dir}/dim_store.csv", index=False)
    
    # Save fact tables
    sales_fact.to_csv(f"{output_dir}/fact_sales.csv", index=False)
    performance_fact.to_csv(f"{output_dir}/fact_performance.csv", index=False)
    
    print(f"Star schema tables saved to {output_dir}")

def main():
    """Main function to implement star schema data model."""
    print("Starting star schema data model implementation...")
    
    # Define input and output directories
    input_dir = "src/data/processed_data"
    output_dir = "src/models/star_schema"
    
    # Load processed data
    products_df, customers_df, stores_df, sales_df, performance_df = load_processed_data(input_dir)
    
    # Create dimension tables
    date_dim = create_date_dimension()
    product_dim = create_product_dimension(products_df)
    customer_dim = create_customer_dimension(customers_df)
    store_dim = create_store_dimension(stores_df)
    
    # Create fact tables
    sales_fact = create_sales_fact(sales_df, product_dim, customer_dim, store_dim, date_dim)
    performance_fact = create_performance_fact(performance_df, product_dim, date_dim)
    
    # Save star schema tables
    save_star_schema(
        (date_dim, product_dim, customer_dim, store_dim, sales_fact, performance_fact),
        output_dir
    )
    
    # Print schema summary
    print("\nStar Schema Summary:")
    print(f"Dimension Tables:")
    print(f"  - Date Dimension: {len(date_dim)} records")
    print(f"  - Product Dimension: {len(product_dim)} records")
    print(f"  - Customer Dimension: {len(customer_dim)} records")
    print(f"  - Store Dimension: {len(store_dim)} records")
    print(f"Fact Tables:")
    print(f"  - Sales Fact: {len(sales_fact)} records")
    print(f"  - Performance Fact: {len(performance_fact)} records")
    
    print("Star schema data model implementation completed successfully!")

if __name__ == "__main__":
    main()
