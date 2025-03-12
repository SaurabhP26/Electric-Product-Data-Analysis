#!/usr/bin/env python3
"""
Generate sample electric product data for analysis.
This script creates realistic sample data that mimics electric product sales,
performance metrics, and customer information.
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import random

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Define constants
NUM_PRODUCTS = 100
NUM_CUSTOMERS = 1000
NUM_STORES = 50
NUM_TRANSACTIONS = 10000
START_DATE = datetime(2022, 1, 1)
END_DATE = datetime(2023, 12, 31)

# Create directories if they don't exist
os.makedirs('sample_data', exist_ok=True)

def generate_product_data():
    """Generate sample product data."""
    categories = ['Lighting', 'Appliances', 'HVAC', 'Electronics', 'Renewable Energy']
    subcategories = {
        'Lighting': ['LED Bulbs', 'Smart Lighting', 'Fixtures', 'Outdoor Lighting', 'Emergency Lighting'],
        'Appliances': ['Refrigerators', 'Washing Machines', 'Dishwashers', 'Ovens', 'Microwaves'],
        'HVAC': ['Air Conditioners', 'Heaters', 'Fans', 'Air Purifiers', 'Thermostats'],
        'Electronics': ['TVs', 'Computers', 'Smartphones', 'Audio Systems', 'Gaming Consoles'],
        'Renewable Energy': ['Solar Panels', 'Wind Turbines', 'Batteries', 'Inverters', 'Chargers']
    }
    
    products = []
    for i in range(1, NUM_PRODUCTS + 1):
        product_id = f'P{i:04d}'
        category = random.choice(categories)
        subcategory = random.choice(subcategories[category])
        energy_rating = random.choice(['A+++', 'A++', 'A+', 'A', 'B', 'C'])
        price = round(random.uniform(10, 2000), 2)
        manufacturing_cost = round(price * random.uniform(0.4, 0.7), 2)
        warranty_years = random.choice([1, 2, 3, 5])
        weight_kg = round(random.uniform(0.1, 100), 2)
        stock_quantity = random.randint(0, 1000)
        
        products.append({
            'product_id': product_id,
            'product_name': f'{subcategory} {random.choice(["Pro", "Max", "Ultra", "Eco", "Basic"])} {random.randint(100, 999)}',
            'category': category,
            'subcategory': subcategory,
            'energy_rating': energy_rating,
            'price': price,
            'manufacturing_cost': manufacturing_cost,
            'warranty_years': warranty_years,
            'weight_kg': weight_kg,
            'stock_quantity': stock_quantity,
            'launch_date': (START_DATE + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
        })
    
    df_products = pd.DataFrame(products)
    df_products.to_csv('sample_data/products.csv', index=False)
    print(f"Generated {len(df_products)} product records")
    return df_products

def generate_customer_data():
    """Generate sample customer data."""
    countries = ['USA', 'Canada', 'UK', 'Germany', 'France', 'Australia', 'Japan', 'China', 'India', 'Brazil']
    segments = ['Residential', 'Commercial', 'Industrial']
    
    customers = []
    for i in range(1, NUM_CUSTOMERS + 1):
        customer_id = f'C{i:04d}'
        country = random.choice(countries)
        segment = random.choice(segments)
        acquisition_date = START_DATE + timedelta(days=random.randint(0, 730))
        
        customers.append({
            'customer_id': customer_id,
            'customer_name': f'Customer {i}',
            'country': country,
            'segment': segment,
            'acquisition_date': acquisition_date.strftime('%Y-%m-%d'),
            'lifetime_value': round(random.uniform(100, 50000), 2)
        })
    
    df_customers = pd.DataFrame(customers)
    df_customers.to_csv('sample_data/customers.csv', index=False)
    print(f"Generated {len(df_customers)} customer records")
    return df_customers

def generate_store_data():
    """Generate sample store data."""
    countries = ['USA', 'Canada', 'UK', 'Germany', 'France', 'Australia', 'Japan', 'China', 'India', 'Brazil']
    store_types = ['Flagship', 'Mall', 'Street', 'Online']
    
    stores = []
    for i in range(1, NUM_STORES + 1):
        store_id = f'S{i:03d}'
        country = random.choice(countries)
        store_type = random.choice(store_types)
        opening_date = START_DATE + timedelta(days=random.randint(-365, 365))
        
        stores.append({
            'store_id': store_id,
            'store_name': f'{country} {store_type} Store {i}',
            'country': country,
            'store_type': store_type,
            'opening_date': opening_date.strftime('%Y-%m-%d'),
            'size_sqm': random.randint(50, 2000) if store_type != 'Online' else 0
        })
    
    df_stores = pd.DataFrame(stores)
    df_stores.to_csv('sample_data/stores.csv', index=False)
    print(f"Generated {len(df_stores)} store records")
    return df_stores

def generate_sales_data(products, customers, stores):
    """Generate sample sales transaction data."""
    payment_methods = ['Credit Card', 'Debit Card', 'Cash', 'Bank Transfer', 'Mobile Payment']
    
    sales = []
    for i in range(1, NUM_TRANSACTIONS + 1):
        transaction_id = f'T{i:06d}'
        transaction_date = START_DATE + timedelta(days=random.randint(0, 730))
        customer_id = random.choice(customers['customer_id'])
        store_id = random.choice(stores['store_id'])
        
        # Each transaction can have multiple products
        num_products_in_transaction = random.randint(1, 5)
        transaction_products = random.sample(list(products['product_id']), num_products_in_transaction)
        
        for product_id in transaction_products:
            product_price = float(products.loc[products['product_id'] == product_id, 'price'].values[0])
            quantity = random.randint(1, 5)
            discount = round(random.uniform(0, 0.3), 2)
            final_price = round(product_price * (1 - discount), 2)
            
            sales.append({
                'transaction_id': transaction_id,
                'product_id': product_id,
                'customer_id': customer_id,
                'store_id': store_id,
                'transaction_date': transaction_date.strftime('%Y-%m-%d'),
                'quantity': quantity,
                'unit_price': product_price,
                'discount': discount,
                'final_price': final_price,
                'total_amount': round(final_price * quantity, 2),
                'payment_method': random.choice(payment_methods)
            })
    
    df_sales = pd.DataFrame(sales)
    df_sales.to_csv('sample_data/sales.csv', index=False)
    print(f"Generated {len(df_sales)} sales records")
    return df_sales

def generate_product_performance_data(products):
    """Generate sample product performance metrics."""
    performance = []
    for _, product in products.iterrows():
        product_id = product['product_id']
        
        # Generate monthly performance data
        current_date = START_DATE
        while current_date <= END_DATE:
            year_month = current_date.strftime('%Y-%m')
            
            performance.append({
                'product_id': product_id,
                'year_month': year_month,
                'energy_consumption_kwh': round(random.uniform(10, 500), 2),
                'failure_rate': round(random.uniform(0, 0.05), 4),
                'customer_satisfaction': round(random.uniform(3, 5), 1),
                'return_rate': round(random.uniform(0, 0.1), 4),
                'warranty_claims': random.randint(0, 10)
            })
            
            # Move to next month
            if current_date.month == 12:
                current_date = datetime(current_date.year + 1, 1, 1)
            else:
                current_date = datetime(current_date.year, current_date.month + 1, 1)
    
    df_performance = pd.DataFrame(performance)
    df_performance.to_csv('sample_data/product_performance.csv', index=False)
    print(f"Generated {len(df_performance)} product performance records")
    return df_performance

def main():
    """Main function to generate all sample datasets."""
    print("Generating sample electric product data...")
    
    # Generate dimension tables
    products = generate_product_data()
    customers = generate_customer_data()
    stores = generate_store_data()
    
    # Generate fact tables
    sales = generate_sales_data(products, customers, stores)
    performance = generate_product_performance_data(products)
    
    print("Sample data generation complete!")
    print(f"Files saved in the sample_data directory:")
    print("  - products.csv")
    print("  - customers.csv")
    print("  - stores.csv")
    print("  - sales.csv")
    print("  - product_performance.csv")

if __name__ == "__main__":
    main()
