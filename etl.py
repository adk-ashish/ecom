import pandas as pd
from sqlalchemy import create_engine
import logging
import matplotlib.pyplot as plt

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database configuration
DB_USERNAME = 'username'
DB_PASSWORD = 'password'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'ecommerce_db'

# PostgreSQL connection string
DATABASE_URL = f'postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# ETL Functions
def extract(file_path):
    """Extract data from a CSV file."""
    logging.info("Starting data extraction")
    try:
        data = pd.read_csv(file_path)
        logging.info("Data extraction successful")
        return data
    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        raise

def transform(data):
    """Transform the raw data."""
    logging.info("Starting data transformation")
    try:
        # Handle missing values
        data.fillna({'quantity': 0, 'price': 0.0}, inplace=True)

        # Convert date column to datetime
        data['date'] = pd.to_datetime(data['date'], errors='coerce')

        # Remove duplicates
        data.drop_duplicates(inplace=True)

        # Add calculated column for total price
        data['total_price'] = data['quantity'] * data['price']

        logging.info("Data transformation successful")
        return data
    except Exception as e:
        logging.error(f"Error during transformation: {e}")
        raise

def load(data, table_name, database_url):
    """Load the transformed data into PostgreSQL."""
    logging.info("Starting data loading")
    try:
        engine = create_engine(database_url)
        data.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info("Data loading successful")
    except Exception as e:
        logging.error(f"Error during loading: {e}")
        raise

def perform_sales_analysis(database_url):
    """Perform sales analysis on the loaded data."""
    logging.info("Starting sales analysis")
    try:
        engine = create_engine(database_url)
        query = """
        SELECT product_name, SUM(total_price) as total_sales
        FROM sales_data
        GROUP BY product_name
        ORDER BY total_sales DESC;
        """
        result = pd.read_sql(query, engine)
        logging.info("Sales analysis completed")
        return result
    except Exception as e:
        logging.error(f"Error during sales analysis: {e}")
        raise

def visualize_sales(data):
    """Visualize sales data using bar charts."""
    logging.info("Starting data visualization")
    try:
        # Plot total sales by product
        plt.figure(figsize=(10, 6))
        plt.bar(data['product_name'], data['total_sales'], color='skyblue')
        plt.xlabel('Product Name')
        plt.ylabel('Total Sales')
        plt.title('Total Sales by Product')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.show()

        logging.info("Data visualization completed")
    except Exception as e:
        logging.error(f"Error during visualization: {e}")
        raise

# Main ETL Process
def main():
    # File path to the sales data
    file_path = 'sales_data.csv'

    # Table name for PostgreSQL
    table_name = 'sales_data'

    try:
        # Step 1: Extract
        raw_data = extract(file_path)

        # Step 2: Transform
        transformed_data = transform(raw_data)

        # Step 3: Load
        load(transformed_data, table_name, DATABASE_URL)

        # Step 4: Analyze
        analysis_result = perform_sales_analysis(DATABASE_URL)
        print("Sales Analysis Results:")
        print(analysis_result)

        # Step 5: Visualize
        visualize_sales(analysis_result)

    except Exception as e:
        logging.error(f"ETL process failed: {e}")

if __name__ == '__main__':
    main()
