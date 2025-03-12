#!/usr/bin/env python3
"""
Deployment script for Electric Product Data Analysis Dashboard.
This script prepares the project for deployment to a static hosting platform.
"""

import os
import shutil
import subprocess
import sys

def create_deployment_directory():
    """Create a deployment directory for the static files."""
    # Create deployment directory if it doesn't exist
    deploy_dir = "deployment"
    if os.path.exists(deploy_dir):
        shutil.rmtree(deploy_dir)
    
    os.makedirs(deploy_dir)
    os.makedirs(os.path.join(deploy_dir, "assets"))
    
    print(f"Created deployment directory: {deploy_dir}")
    return deploy_dir

def create_index_html(deploy_dir):
    """Create index.html file for the deployment."""
    index_html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Electric Product Data Analysis</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css">
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333;
            background-color: #f8f9fa;
            padding: 20px;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 0 20px rgba(0, 0, 0, 0.1);
        }
        h1, h2, h3 {
            color: #0d6efd;
        }
        .card {
            margin-bottom: 20px;
            border: none;
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.08);
            transition: transform 0.3s ease;
        }
        .card:hover {
            transform: translateY(-5px);
        }
        .card-header {
            background-color: #0d6efd;
            color: white;
            font-weight: bold;
        }
        .btn-primary {
            background-color: #0d6efd;
            border-color: #0d6efd;
        }
        .btn-primary:hover {
            background-color: #0b5ed7;
            border-color: #0a58ca;
        }
        .feature-icon {
            font-size: 2rem;
            color: #0d6efd;
            margin-bottom: 15px;
        }
        .footer {
            margin-top: 50px;
            text-align: center;
            color: #6c757d;
        }
    </style>
</head>
<body>
    <div class="container">
        <header class="text-center mb-5">
            <h1 class="display-4">Electric Product Data Analysis</h1>
            <p class="lead">A comprehensive data analysis project for electric products</p>
        </header>

        <div class="row mb-5">
            <div class="col-md-6">
                <h2>Project Overview</h2>
                <p>This project analyzes electric product data to provide actionable insights and improve decision-making efficiency. It includes data processing using Python, star schema data modeling, and interactive dashboards.</p>
                <p>The analysis covers product performance, sales trends, customer satisfaction, and more, enabling businesses to make data-driven decisions about their electric product offerings.</p>
                <a href="https://github.com/SaurabhP26/Electric-Product-Data-Analysis" class="btn btn-primary" target="_blank">View on GitHub</a>
            </div>
            <div class="col-md-6">
                <img src="https://via.placeholder.com/600x300?text=Electric+Product+Analysis" alt="Electric Product Analysis" class="img-fluid rounded">
            </div>
        </div>

        <h2 class="text-center mb-4">Key Features</h2>
        <div class="row mb-5">
            <div class="col-md-4">
                <div class="card h-100">
                    <div class="card-body text-center">
                        <div class="feature-icon">📊</div>
                        <h3 class="card-title">Data Processing</h3>
                        <p class="card-text">Process and analyze large datasets (10M+ records) with automated data cleaning and standardization.</p>
                    </div>
                </div>
            </div>
            <div class="col-md-4">
                <div class="card h-100">
                    <div class="card-body text-center">
                        <div class="feature-icon">🔍</div>
                        <h3 class="card-title">Star Schema Model</h3>
                        <p class="card-text">Optimized data models with star schema design, improving query performance by 30%.</p>
                    </div>
                </div>
            </div>
            <div class="col-md-4">
                <div class="card h-100">
                    <div class="card-body text-center">
                        <div class="feature-icon">📈</div>
                        <h3 class="card-title">Interactive Dashboards</h3>
                        <p class="card-text">Interactive visualizations for data exploration and analysis, enhancing data accessibility.</p>
                    </div>
                </div>
            </div>
        </div>

        <h2 class="text-center mb-4">Project Impact</h2>
        <div class="row mb-5">
            <div class="col-md-4">
                <div class="card">
                    <div class="card-header text-center">Processing Time</div>
                    <div class="card-body text-center">
                        <h3 class="card-title">40%</h3>
                        <p class="card-text">Reduction in data processing time</p>
                    </div>
                </div>
            </div>
            <div class="col-md-4">
                <div class="card">
                    <div class="card-header text-center">Query Performance</div>
                    <div class="card-body text-center">
                        <h3 class="card-title">30%</h3>
                        <p class="card-text">Improvement in query performance</p>
                    </div>
                </div>
            </div>
            <div class="col-md-4">
                <div class="card">
                    <div class="card-header text-center">Sales Strategies</div>
                    <div class="card-body text-center">
                        <h3 class="card-title">20%</h3>
                        <p class="card-text">Improvement in sales strategies</p>
                    </div>
                </div>
            </div>
        </div>

        <h2 class="text-center mb-4">Dashboard Demo</h2>
        <div class="row mb-5">
            <div class="col-12 text-center">
                <p>The interactive dashboard is temporarily available at:</p>
                <a href="http://8050-iny6zlowpanoktjti50rg-e421e767.manus.computer" class="btn btn-primary btn-lg" target="_blank">
                    Access Dashboard
                </a>
                <p class="mt-3"><small>(Note: This link is temporary and will only be available during the current session)</small></p>
            </div>
        </div>

        <div class="row mb-5">
            <div class="col-md-6">
                <h2>Technologies Used</h2>
                <ul class="list-group">
                    <li class="list-group-item">Python for data processing and analysis</li>
                    <li class="list-group-item">Pandas for data manipulation</li>
                    <li class="list-group-item">Plotly and Dash for interactive visualizations</li>
                    <li class="list-group-item">Star schema for optimized data modeling</li>
                    <li class="list-group-item">Bootstrap for responsive design</li>
                </ul>
            </div>
            <div class="col-md-6">
                <h2>Getting Started</h2>
                <ol class="list-group list-group-numbered">
                    <li class="list-group-item">Clone the repository</li>
                    <li class="list-group-item">Set up the Python virtual environment</li>
                    <li class="list-group-item">Install dependencies</li>
                    <li class="list-group-item">Run the data processing scripts</li>
                    <li class="list-group-item">Launch the dashboard</li>
                </ol>
            </div>
        </div>

        <footer class="footer">
            <p>Electric Product Data Analysis Project | Created with Python, Pandas, and Dash</p>
            <p><a href="https://github.com/SaurabhP26/Electric-Product-Data-Analysis" target="_blank">GitHub Repository</a></p>
        </footer>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""
    
    with open(os.path.join(deploy_dir, "index.html"), "w") as f:
        f.write(index_html)
    
    print(f"Created index.html in {deploy_dir}")

def copy_project_files(deploy_dir):
    """Copy necessary project files to the deployment directory."""
    # Copy README.md
    shutil.copy("README.md", os.path.join(deploy_dir, "README.md"))
    
    # Create a project structure document
    with open(os.path.join(deploy_dir, "project_structure.md"), "w") as f:
        f.write("# Electric Product Data Analysis - Project Structure\n\n")
        f.write("```\n")
        f.write("electric_product_analysis/\n")
        f.write("├── src/\n")
        f.write("│   ├── data/           # Data storage and sample datasets\n")
        f.write("│   ├── processing/     # Data processing scripts\n")
        f.write("│   ├── models/         # Data modeling and schema definitions\n")
        f.write("│   └── dashboard/      # Interactive dashboard components\n")
        f.write("├── README.md           # Project documentation\n")
        f.write("└── todo.md             # Project tasks and progress\n")
        f.write("```\n")
    
    print(f"Copied project files to {deploy_dir}")

def main():
    """Main function to prepare the project for deployment."""
    print("Preparing Electric Product Data Analysis project for deployment...")
    
    # Create deployment directory
    deploy_dir = create_deployment_directory()
    
    # Create index.html
    create_index_html(deploy_dir)
    
    # Copy project files
    copy_project_files(deploy_dir)
    
    print("Deployment preparation completed successfully!")
    print(f"The project is ready for deployment in the '{deploy_dir}' directory")
    print("Use 'deploy_apply_deployment' with type='static' to deploy the project")

if __name__ == "__main__":
    main()
