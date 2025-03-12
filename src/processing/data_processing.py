#!/usr/bin/env python3
"""
PySpark data processing script for electric product data.
This script performs data cleaning, standardization, and transformation
on the sample electric product dataset.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, trim, upper, lower
from pyspark.sql.functions import year, month, dayofmonth, date_format, to_date
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import DoubleType, IntegerType
import os

def initialize_spark():
    """Initialize Spark session."""
    spark = SparkSession.builder \
        .appName("Electric Product Data Processing") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    print("Spark session initialized")
    return spark

def load_data(spark, data_dir):
    """Load data from CSV files."""
    products_df = spark.read.csv(f"{data_dir}/products.csv", header=True, inferSchema=True)
    customers_df = spark.read.csv(f"{data_dir}/customers.csv", header=True, inferSchema=True)
    stores_df = spark.read.csv(f"{data_dir}/stores.csv", header=True, inferSchema=True)
    sales_df = spark.read.csv(f"{data_dir}/sales.csv", header=True, inferSchema=True)
    performance_df = spark.read.csv(f"{data_dir}/product_performance.csv", header=True, inferSchema=True)
    
    print(f"Loaded data from {data_dir}")
    print(f"Products: {products_df.count()} records")
    print(f"Customers: {customers_df.count()} records")
    print(f"Stores: {stores_df.count()} records")
    print(f"Sales: {sales_df.count()} records")
    print(f"Performance: {performance_df.count()} records")
    
    return products_df, customers_df, stores_df, sales_df, performance_df

def clean_products_data(df):
    """Clean and standardize products data."""
    # Handle missing values
    df = df.na.fill({"warranty_years": 1, "weight_kg": 0.0, "stock_quantity": 0})
    
    # Standardize text fields
    df = df.withColumn("product_name", trim(df.product_name))
    df = df.withColumn("category", upper(df.category))
    df = df.withColumn("subcategory", trim(df.subcategory))
    
    # Convert data types
    df = df.withColumn("price", col("price").cast(DoubleType()))
    df = df.withColumn("manufacturing_cost", col("manufacturing_cost").cast(DoubleType()))
    df = df.withColumn("weight_kg", col("weight_kg").cast(DoubleType()))
    df = df.withColumn("stock_quantity", col("stock_quantity").cast(IntegerType()))
    
    # Add derived columns
    df = df.withColumn("profit_margin", 
                      spark_round((col("price") - col("manufacturing_cost")) / col("price") * 100, 2))
    
    # Convert date strings to date type
    df = df.withColumn("launch_date", to_date(col("launch_date"), "yyyy-MM-dd"))
    
    print("Products data cleaned and standardized")
    return df

def clean_customers_data(df):
    """Clean and standardize customers data."""
    # Handle missing values
    df = df.na.fill({"lifetime_value": 0.0})
    
    # Standardize text fields
    df = df.withColumn("customer_name", trim(df.customer_name))
    df = df.withColumn("country", upper(df.country))
    df = df.withColumn("segment", trim(df.segment))
    
    # Convert data types
    df = df.withColumn("lifetime_value", col("lifetime_value").cast(DoubleType()))
    
    # Convert date strings to date type
    df = df.withColumn("acquisition_date", to_date(col("acquisition_date"), "yyyy-MM-dd"))
    
    # Add derived columns
    df = df.withColumn("acquisition_year", year(col("acquisition_date")))
    df = df.withColumn("acquisition_month", month(col("acquisition_date")))
    
    print("Customers data cleaned and standardized")
    return df

def clean_stores_data(df):
    """Clean and standardize stores data."""
    # Handle missing values
    df = df.na.fill({"size_sqm": 0})
    
    # Standardize text fields
    df = df.withColumn("store_name", trim(df.store_name))
    df = df.withColumn("country", upper(df.country))
    df = df.withColumn("store_type", trim(df.store_type))
    
    # Convert data types
    df = df.withColumn("size_sqm", col("size_sqm").cast(IntegerType()))
    
    # Convert date strings to date type
    df = df.withColumn("opening_date", to_date(col("opening_date"), "yyyy-MM-dd"))
    
    # Add derived columns
    df = df.withColumn("opening_year", year(col("opening_date")))
    
    print("Stores data cleaned and standardized")
    return df

def clean_sales_data(df):
    """Clean and standardize sales data."""
    # Handle missing values
    df = df.na.fill({"quantity": 1, "discount": 0.0})
    
    # Convert data types
    df = df.withColumn("quantity", col("quantity").cast(IntegerType()))
    df = df.withColumn("unit_price", col("unit_price").cast(DoubleType()))
    df = df.withColumn("discount", col("discount").cast(DoubleType()))
    df = df.withColumn("final_price", col("final_price").cast(DoubleType()))
    df = df.withColumn("total_amount", col("total_amount").cast(DoubleType()))
    
    # Standardize text fields
    df = df.withColumn("payment_method", trim(df.payment_method))
    
    # Convert date strings to date type
    df = df.withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
    
    # Add derived columns
    df = df.withColumn("transaction_year", year(col("transaction_date")))
    df = df.withColumn("transaction_month", month(col("transaction_date")))
    df = df.withColumn("transaction_day", dayofmonth(col("transaction_date")))
    
    print("Sales data cleaned and standardized")
    return df

def clean_performance_data(df):
    """Clean and standardize product performance data."""
    # Handle missing values
    df = df.na.fill({
        "energy_consumption_kwh": 0.0,
        "failure_rate": 0.0,
        "customer_satisfaction": 3.0,
        "return_rate": 0.0,
        "warranty_claims": 0
    })
    
    # Convert data types
    df = df.withColumn("energy_consumption_kwh", col("energy_consumption_kwh").cast(DoubleType()))
    df = df.withColumn("failure_rate", col("failure_rate").cast(DoubleType()))
    df = df.withColumn("customer_satisfaction", col("customer_satisfaction").cast(DoubleType()))
    df = df.withColumn("return_rate", col("return_rate").cast(DoubleType()))
    df = df.withColumn("warranty_claims", col("warranty_claims").cast(IntegerType()))
    
    # Extract year and month from year_month field
    df = df.withColumn("year", col("year_month").substr(1, 4).cast(IntegerType()))
    df = df.withColumn("month", col("year_month").substr(6, 2).cast(IntegerType()))
    
    print("Performance data cleaned and standardized")
    return df

def save_processed_data(dfs, output_dir):
    """Save processed dataframes to parquet files."""
    products_df, customers_df, stores_df, sales_df, performance_df = dfs
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Save dataframes to parquet files
    products_df.write.mode("overwrite").parquet(f"{output_dir}/products")
    customers_df.write.mode("overwrite").parquet(f"{output_dir}/customers")
    stores_df.write.mode("overwrite").parquet(f"{output_dir}/stores")
    sales_df.write.mode("overwrite").parquet(f"{output_dir}/sales")
    performance_df.write.mode("overwrite").parquet(f"{output_dir}/performance")
    
    print(f"Processed data saved to {output_dir}")

def main():
    """Main function to process electric product data."""
    print("Starting electric product data processing...")
    
    # Initialize Spark
    spark = initialize_spark()
    
    # Define input and output directories
    input_dir = "src/data/sample_data"
    output_dir = "src/data/processed_data"
    
    # Load data
    products_df, customers_df, stores_df, sales_df, performance_df = load_data(spark, input_dir)
    
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
    products_df_clean.show(5)
    
    print("\nSample of processed sales data:")
    sales_df_clean.show(5)
    
    print("Electric product data processing completed successfully!")
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
