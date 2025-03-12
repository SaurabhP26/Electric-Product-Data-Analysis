#!/usr/bin/env python3
"""
Interactive Dashboard for Electric Product Data Analysis.
This script creates a Dash web application with interactive visualizations
for analyzing electric product data.
"""

import pandas as pd
import numpy as np
import os
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, html, dcc, callback, Output, Input
import dash_bootstrap_components as dbc

# Define paths
DATA_DIR = "src/models/star_schema"

def load_star_schema_data(data_dir):
    """Load star schema data from CSV files."""
    # Load dimension tables
    dim_date = pd.read_csv(f"{data_dir}/dim_date.csv")
    dim_product = pd.read_csv(f"{data_dir}/dim_product.csv")
    dim_customer = pd.read_csv(f"{data_dir}/dim_customer.csv")
    dim_store = pd.read_csv(f"{data_dir}/dim_store.csv")
    
    # Load fact tables
    fact_sales = pd.read_csv(f"{data_dir}/fact_sales.csv")
    fact_performance = pd.read_csv(f"{data_dir}/fact_performance.csv")
    
    print("Star schema data loaded successfully")
    return dim_date, dim_product, dim_customer, dim_store, fact_sales, fact_performance

def prepare_dashboard_data(dim_date, dim_product, dim_customer, dim_store, fact_sales, fact_performance):
    """Prepare data for dashboard visualizations by joining fact and dimension tables."""
    # Join sales fact with dimensions
    sales_data = fact_sales.merge(dim_date, on='date_key', how='left')
    sales_data = sales_data.merge(dim_product, on='product_key', how='left')
    sales_data = sales_data.merge(dim_customer, on='customer_key', how='left')
    sales_data = sales_data.merge(dim_store, on='store_key', how='left')
    
    # Join performance fact with dimensions
    performance_data = fact_performance.merge(dim_date, on='date_key', how='left')
    performance_data = performance_data.merge(dim_product, on='product_key', how='left')
    
    # Convert date columns to datetime
    sales_data['date'] = pd.to_datetime(sales_data['date'])
    performance_data['date'] = pd.to_datetime(performance_data['date'])
    
    print("Dashboard data prepared successfully")
    return sales_data, performance_data

def create_app(sales_data, performance_data):
    """Create Dash application with interactive visualizations."""
    # Initialize Dash app
    app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
    
    # Get unique categories and countries for filters
    categories = sorted(sales_data['category'].unique())
    countries = sorted(sales_data['country_y'].unique())
    years = sorted(sales_data['year'].unique())
    
    # Create app layout
    app.layout = dbc.Container([
        dbc.Row([
            dbc.Col([
                html.H1("Electric Product Data Analysis Dashboard", 
                        className="text-center text-primary mb-4"),
                html.P("Interactive dashboard for analyzing electric product sales and performance metrics.",
                       className="text-center text-muted")
            ], width=12)
        ]),
        
        dbc.Row([
            dbc.Col([
                html.H4("Filters", className="text-primary"),
                html.Label("Select Category:"),
                dcc.Dropdown(
                    id='category-filter',
                    options=[{'label': cat, 'value': cat} for cat in categories],
                    value=categories,
                    multi=True
                ),
                html.Label("Select Country:"),
                dcc.Dropdown(
                    id='country-filter',
                    options=[{'label': country, 'value': country} for country in countries],
                    value=countries,
                    multi=True
                ),
                html.Label("Select Year:"),
                dcc.Dropdown(
                    id='year-filter',
                    options=[{'label': str(year), 'value': year} for year in years],
                    value=years,
                    multi=True
                )
            ], width=3),
            
            dbc.Col([
                html.H4("Key Metrics", className="text-primary"),
                dbc.Row([
                    dbc.Col(dbc.Card([
                        dbc.CardBody([
                            html.H5("Total Sales", className="card-title"),
                            html.H3(id="total-sales", className="text-center text-success")
                        ])
                    ]), width=4),
                    dbc.Col(dbc.Card([
                        dbc.CardBody([
                            html.H5("Avg. Customer Satisfaction", className="card-title"),
                            html.H3(id="avg-satisfaction", className="text-center text-info")
                        ])
                    ]), width=4),
                    dbc.Col(dbc.Card([
                        dbc.CardBody([
                            html.H5("Profit Margin", className="card-title"),
                            html.H3(id="avg-profit-margin", className="text-center text-warning")
                        ])
                    ]), width=4)
                ]),
                
                html.H4("Sales Trends", className="text-primary mt-4"),
                dcc.Graph(id="sales-trend-chart")
            ], width=9)
        ]),
        
        dbc.Row([
            dbc.Col([
                html.H4("Sales by Category", className="text-primary"),
                dcc.Graph(id="category-sales-chart")
            ], width=6),
            
            dbc.Col([
                html.H4("Sales by Country", className="text-primary"),
                dcc.Graph(id="country-sales-chart")
            ], width=6)
        ]),
        
        dbc.Row([
            dbc.Col([
                html.H4("Product Performance Metrics", className="text-primary"),
                dcc.Graph(id="performance-metrics-chart")
            ], width=12)
        ]),
        
        dbc.Row([
            dbc.Col([
                html.H4("Energy Consumption vs. Customer Satisfaction", className="text-primary"),
                dcc.Graph(id="energy-satisfaction-chart")
            ], width=6),
            
            dbc.Col([
                html.H4("Failure Rate by Product Category", className="text-primary"),
                dcc.Graph(id="failure-rate-chart")
            ], width=6)
        ]),
        
        html.Footer([
            html.P("Electric Product Data Analysis Dashboard | Created with Dash and Plotly",
                   className="text-center text-muted mt-4")
        ])
    ], fluid=True)
    
    # Define callbacks for interactive elements
    @app.callback(
        [Output("total-sales", "children"),
         Output("avg-satisfaction", "children"),
         Output("avg-profit-margin", "children"),
         Output("sales-trend-chart", "figure"),
         Output("category-sales-chart", "figure"),
         Output("country-sales-chart", "figure"),
         Output("performance-metrics-chart", "figure"),
         Output("energy-satisfaction-chart", "figure"),
         Output("failure-rate-chart", "figure")],
        [Input("category-filter", "value"),
         Input("country-filter", "value"),
         Input("year-filter", "value")]
    )
    def update_dashboard(selected_categories, selected_countries, selected_years):
        # Filter data based on selections
        filtered_sales = sales_data[
            (sales_data['category'].isin(selected_categories)) &
            (sales_data['country_y'].isin(selected_countries)) &
            (sales_data['year'].isin(selected_years))
        ]
        
        filtered_performance = performance_data[
            (performance_data['category'].isin(selected_categories)) &
            (performance_data['year'].isin(selected_years))
        ]
        
        # Calculate key metrics
        total_sales = f"${filtered_sales['total_amount'].sum():,.2f}"
        
        avg_satisfaction = filtered_performance['customer_satisfaction'].mean()
        avg_satisfaction = f"{avg_satisfaction:.1f}/5.0" if not pd.isna(avg_satisfaction) else "N/A"
        
        avg_profit_margin = filtered_sales['profit_margin'].mean()
        avg_profit_margin = f"{avg_profit_margin:.1f}%" if not pd.isna(avg_profit_margin) else "N/A"
        
        # Create sales trend chart
        sales_by_month = filtered_sales.groupby(['year', 'month'])['total_amount'].sum().reset_index()
        sales_by_month['date'] = pd.to_datetime(sales_by_month['year'].astype(str) + '-' + 
                                               sales_by_month['month'].astype(str) + '-01')
        sales_by_month = sales_by_month.sort_values('date')
        
        sales_trend_fig = px.line(
            sales_by_month, x='date', y='total_amount',
            title='Monthly Sales Trend',
            labels={'total_amount': 'Total Sales ($)', 'date': 'Date'},
            template='plotly_white'
        )
        
        # Create category sales chart
        category_sales = filtered_sales.groupby('category')['total_amount'].sum().reset_index()
        category_sales = category_sales.sort_values('total_amount', ascending=False)
        
        category_sales_fig = px.bar(
            category_sales, x='category', y='total_amount',
            title='Sales by Product Category',
            labels={'total_amount': 'Total Sales ($)', 'category': 'Category'},
            template='plotly_white',
            color='category'
        )
        
        # Create country sales chart
        country_sales = filtered_sales.groupby('country_y')['total_amount'].sum().reset_index()
        country_sales = country_sales.sort_values('total_amount', ascending=False)
        
        country_sales_fig = px.bar(
            country_sales, x='country_y', y='total_amount',
            title='Sales by Country',
            labels={'total_amount': 'Total Sales ($)', 'country_y': 'Country'},
            template='plotly_white',
            color='country_y'
        )
        
        # Create performance metrics chart
        performance_metrics = filtered_performance.groupby('category').agg({
            'energy_consumption_kwh': 'mean',
            'failure_rate': 'mean',
            'customer_satisfaction': 'mean',
            'return_rate': 'mean'
        }).reset_index()
        
        performance_metrics_fig = px.bar(
            performance_metrics, x='category', 
            y=['energy_consumption_kwh', 'failure_rate', 'customer_satisfaction', 'return_rate'],
            title='Average Performance Metrics by Category',
            labels={'value': 'Value', 'category': 'Category', 'variable': 'Metric'},
            template='plotly_white',
            barmode='group'
        )
        
        # Create energy consumption vs. satisfaction chart
        energy_satisfaction_fig = px.scatter(
            filtered_performance, x='energy_consumption_kwh', y='customer_satisfaction',
            color='category', size='return_rate',
            title='Energy Consumption vs. Customer Satisfaction',
            labels={
                'energy_consumption_kwh': 'Energy Consumption (kWh)',
                'customer_satisfaction': 'Customer Satisfaction (1-5)',
                'category': 'Category',
                'return_rate': 'Return Rate'
            },
            template='plotly_white'
        )
        
        # Create failure rate chart
        failure_rate = filtered_performance.groupby(['category', 'subcategory'])['failure_rate'].mean().reset_index()
        failure_rate = failure_rate.sort_values(['category', 'failure_rate'], ascending=[True, False])
        
        failure_rate_fig = px.bar(
            failure_rate, x='subcategory', y='failure_rate', color='category',
            title='Average Failure Rate by Product Subcategory',
            labels={'failure_rate': 'Failure Rate', 'subcategory': 'Subcategory', 'category': 'Category'},
            template='plotly_white'
        )
        
        return (
            total_sales, avg_satisfaction, avg_profit_margin,
            sales_trend_fig, category_sales_fig, country_sales_fig,
            performance_metrics_fig, energy_satisfaction_fig, failure_rate_fig
        )
    
    return app

def main():
    """Main function to create and run the dashboard."""
    print("Starting Electric Product Data Analysis Dashboard...")
    
    # Load star schema data
    dim_date, dim_product, dim_customer, dim_store, fact_sales, fact_performance = load_star_schema_data(DATA_DIR)
    
    # Prepare data for dashboard
    sales_data, performance_data = prepare_dashboard_data(
        dim_date, dim_product, dim_customer, dim_store, fact_sales, fact_performance
    )
    
    # Create Dash app
    app = create_app(sales_data, performance_data)
    
    print("Dashboard created successfully!")
    print("Run the app with 'app.run_server(host='0.0.0.0', port=8050, debug=True)' to start the server")
    
    return app

if __name__ == "__main__":
    app = main()
    # Run the app
    app.run_server(host='0.0.0.0', port=8050, debug=True)
