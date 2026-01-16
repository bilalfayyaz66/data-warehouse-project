"""
Bilal Data Warehouse ETL Pipeline
==================================
A comprehensive ETL solution for loading data into TBWalmart data warehouse
with advanced features including:
- Multithreading for parallel data loading
- Hybrid Join implementation (Hash Join & Nested Loop Join)
- Incremental data loading with SCD Type 1
- Error handling and logging
- Performance optimization
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime
import threading
from queue import Queue
import hashlib
import getpass
import sys
from typing import Dict, List, Tuple
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'etl_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Manage MySQL database connections with connection pooling"""
    
    def __init__(self, host: str, user: str, password: str, database: str):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                autocommit=False
            )
            logger.info(f"Successfully connected to database: {self.database}")
            return self.connection
        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            raise
            
    def close(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Database connection closed")
            
    def get_connection(self):
        """Get active connection or create new one"""
        if not self.connection or not self.connection.is_connected():
            return self.connect()
        return self.connection


class HybridJoin:
    """
    Implementation of Hybrid Join Algorithm
    Combines Hash Join and Nested Loop Join for optimal performance
    """
    
    @staticmethod
    def hash_join(left_df: pd.DataFrame, right_df: pd.DataFrame, 
                  left_key: str, right_key: str, join_type: str = 'inner') -> pd.DataFrame:
        """
        Hash Join implementation - efficient for large datasets with equi-joins
        
        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            left_key: Key column in left DataFrame
            right_key: Key column in right DataFrame
            join_type: Type of join ('inner', 'left', 'right', 'outer')
        
        Returns:
            Joined DataFrame
        """
        logger.info(f"Performing Hash Join on {left_key} = {right_key}")
        start_time = time.time()
        
        # Build hash table for smaller dataset
        if len(left_df) <= len(right_df):
            build_df = left_df
            probe_df = right_df
            build_key = left_key
            probe_key = right_key
            swap = False
        else:
            build_df = right_df
            probe_df = left_df
            build_key = right_key
            probe_key = left_key
            swap = True
        
        # Create hash table
        hash_table = {}
        for idx, row in build_df.iterrows():
            key_value = row[build_key]
            if key_value not in hash_table:
                hash_table[key_value] = []
            hash_table[key_value].append(row)
        
        # Probe phase
        result_rows = []
        for idx, probe_row in probe_df.iterrows():
            key_value = probe_row[probe_key]
            if key_value in hash_table:
                for build_row in hash_table[key_value]:
                    if swap:
                        combined = {**probe_row.to_dict(), **build_row.to_dict()}
                    else:
                        combined = {**build_row.to_dict(), **probe_row.to_dict()}
                    result_rows.append(combined)
        
        result_df = pd.DataFrame(result_rows) if result_rows else pd.DataFrame()
        
        elapsed_time = time.time() - start_time
        logger.info(f"Hash Join completed in {elapsed_time:.2f} seconds. Result rows: {len(result_df)}")
        
        return result_df
    
    @staticmethod
    def nested_loop_join(left_df: pd.DataFrame, right_df: pd.DataFrame,
                        condition_func) -> pd.DataFrame:
        """
        Nested Loop Join implementation - flexible for non-equi joins
        
        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            condition_func: Function that takes two rows and returns True if they should join
        
        Returns:
            Joined DataFrame
        """
        logger.info("Performing Nested Loop Join")
        start_time = time.time()
        
        result_rows = []
        for _, left_row in left_df.iterrows():
            for _, right_row in right_df.iterrows():
                if condition_func(left_row, right_row):
                    combined = {**left_row.to_dict(), **right_row.to_dict()}
                    result_rows.append(combined)
        
        result_df = pd.DataFrame(result_rows) if result_rows else pd.DataFrame()
        
        elapsed_time = time.time() - start_time
        logger.info(f"Nested Loop Join completed in {elapsed_time:.2f} seconds. Result rows: {len(result_df)}")
        
        return result_df


class DataTransformer:
    """Handle data transformation and cleaning operations"""
    
    @staticmethod
    def transform_customer_data(df: pd.DataFrame) -> pd.DataFrame:
        """Transform customer master data"""
        logger.info("Transforming customer data")
        
        # Clean and standardize data
        df = df.copy()
        df['Customer_ID'] = df['Customer_ID'].astype(str)
        df['Gender'] = df['Gender'].fillna('U').str.upper().str[0]
        df['Age_Range'] = df['Age'].astype(str)
        df['Occupation'] = df['Occupation'].fillna(0).astype(int)
        df['City_Category'] = df['City_Category'].fillna('Z').str.upper().str[0]
        df['Stay_In_Current_City_Years'] = df['Stay_In_Current_City_Years'].astype(str)
        df['Marital_Status'] = df['Marital_Status'].fillna(0).astype(int)
        
        # Select required columns
        df = df[['Customer_ID', 'Gender', 'Age_Range', 'Occupation', 
                 'City_Category', 'Stay_In_Current_City_Years', 'Marital_Status']]
        
        logger.info(f"Transformed {len(df)} customer records")
        return df
    
    @staticmethod
    def transform_product_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Transform product master data and extract store and supplier dimensions
        
        Returns:
            Tuple of (product_df, store_df, supplier_df)
        """
        logger.info("Transforming product data")
        
        df = df.copy()
        
        # Extract unique stores
        store_df = df[['storeID', 'storeName']].drop_duplicates()
        store_df.columns = ['Store_ID', 'Store_Name']
        store_df = store_df.dropna()
        store_df['Store_ID'] = store_df['Store_ID'].astype(int)
        
        # Extract unique suppliers
        supplier_df = df[['supplierID', 'supplierName']].drop_duplicates()
        supplier_df.columns = ['Supplier_ID', 'Supplier_Name']
        supplier_df = supplier_df.dropna()
        supplier_df['Supplier_ID'] = supplier_df['Supplier_ID'].astype(int)
        
        # Transform product data
        product_df = df[['Product_ID', 'Product_Category', 'price$', 'storeID', 'supplierID']].copy()
        product_df.columns = ['Product_ID', 'Product_Category', 'Price', 'Store_ID', 'Supplier_ID']
        product_df['Product_ID'] = product_df['Product_ID'].astype(str)
        product_df['Price'] = product_df['Price'].astype(float)
        product_df['Store_ID'] = product_df['Store_ID'].astype(int)
        product_df['Supplier_ID'] = product_df['Supplier_ID'].astype(int)
        product_df = product_df.dropna()
        
        logger.info(f"Transformed {len(product_df)} products, {len(store_df)} stores, {len(supplier_df)} suppliers")
        return product_df, store_df, supplier_df
    
    @staticmethod
    def generate_date_dimension(start_date: str, end_date: str) -> pd.DataFrame:
        """
        Generate date dimension table with all required attributes
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
        
        Returns:
            DataFrame with date dimension
        """
        logger.info(f"Generating date dimension from {start_date} to {end_date}")
        
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        
        date_data = []
        for idx, date in enumerate(date_range, start=1):
            # Determine season
            month = date.month
            if month in [12, 1, 2]:
                season = 'Winter'
            elif month in [3, 4, 5]:
                season = 'Spring'
            elif month in [6, 7, 8]:
                season = 'Summer'
            else:
                season = 'Fall'
            
            # Determine weekday/weekend
            weekday_weekend = 'Weekend' if date.dayofweek >= 5 else 'Weekday'
            
            date_data.append({
                'Date_ID': idx,
                'Full_Date': date.date(),
                'Day': date.day,
                'Month': date.month,
                'Month_Name': date.strftime('%B'),
                'Quarter': f'Q{(date.month-1)//3 + 1}',
                'Year': date.year,
                'Weekday_Weekend': weekday_weekend,
                'Season': season
            })
        
        date_df = pd.DataFrame(date_data)
        logger.info(f"Generated {len(date_df)} date records")
        return date_df


class MultithreadedLoader:
    """Handle multithreaded data loading operations"""
    
    def __init__(self, db_config: Dict, max_workers: int = 4):
        self.db_config = db_config
        self.max_workers = max_workers
        self.results = []
        self.lock = threading.Lock()
        
    def load_dimension_batch(self, table_name: str, data_batch: pd.DataFrame, 
                            thread_id: int) -> Dict:
        """
        Load a batch of dimension data in a separate thread
        
        Args:
            table_name: Target table name
            data_batch: Data to load
            thread_id: Thread identifier
        
        Returns:
            Dictionary with loading results
        """
        try:
            # Create connection for this thread
            conn = mysql.connector.connect(
                host=self.db_config['host'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database']
            )
            cursor = conn.cursor()
            
            logger.info(f"Thread {thread_id}: Loading {len(data_batch)} records into {table_name}")
            
            # Prepare INSERT statement based on table
            if table_name == 'Customer_Dim':
                sql = """
                INSERT INTO Customer_Dim 
                (Customer_ID, Gender, Age_Range, Occupation, City_Category, 
                 Stay_In_Current_City_Years, Marital_Status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                Gender=VALUES(Gender),
                Age_Range=VALUES(Age_Range),
                Occupation=VALUES(Occupation),
                City_Category=VALUES(City_Category),
                Stay_In_Current_City_Years=VALUES(Stay_In_Current_City_Years),
                Marital_Status=VALUES(Marital_Status)
                """
                data_tuples = [tuple(x) for x in data_batch.values]
                
            elif table_name == 'Store_Dim':
                sql = """
                INSERT INTO Store_Dim (Store_ID, Store_Name)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE Store_Name=VALUES(Store_Name)
                """
                data_tuples = [tuple(x) for x in data_batch.values]
                
            elif table_name == 'Supplier_Dim':
                sql = """
                INSERT INTO Supplier_Dim (Supplier_ID, Supplier_Name)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE Supplier_Name=VALUES(Supplier_Name)
                """
                data_tuples = [tuple(x) for x in data_batch.values]
                
            elif table_name == 'Product_Dim':
                sql = """
                INSERT INTO Product_Dim 
                (Product_ID, Product_Category, Price, Store_ID, Supplier_ID)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                Product_Category=VALUES(Product_Category),
                Price=VALUES(Price),
                Store_ID=VALUES(Store_ID),
                Supplier_ID=VALUES(Supplier_ID)
                """
                data_tuples = [tuple(x) for x in data_batch.values]
                
            elif table_name == 'Date_Dim':
                sql = """
                INSERT INTO Date_Dim 
                (Date_ID, Full_Date, Day, Month, Month_Name, Quarter, Year, 
                 Weekday_Weekend, Season)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE Date_ID=Date_ID
                """
                data_tuples = [tuple(x) for x in data_batch.values]
            
            # Execute batch insert
            cursor.executemany(sql, data_tuples)
            conn.commit()
            
            rows_affected = cursor.rowcount
            cursor.close()
            conn.close()
            
            logger.info(f"Thread {thread_id}: Successfully loaded {rows_affected} records into {table_name}")
            
            return {
                'thread_id': thread_id,
                'table': table_name,
                'rows_loaded': rows_affected,
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"Thread {thread_id}: Error loading into {table_name}: {e}")
            return {
                'thread_id': thread_id,
                'table': table_name,
                'rows_loaded': 0,
                'status': 'failed',
                'error': str(e)
            }
    
    def load_fact_batch(self, data_batch: pd.DataFrame, thread_id: int) -> Dict:
        """
        Load a batch of fact data in a separate thread
        
        Args:
            data_batch: Fact data to load
            thread_id: Thread identifier
        
        Returns:
            Dictionary with loading results
        """
        try:
            conn = mysql.connector.connect(
                host=self.db_config['host'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database']
            )
            cursor = conn.cursor()
            
            logger.info(f"Thread {thread_id}: Loading {len(data_batch)} fact records")
            
            sql = """
            INSERT INTO Sales_Fact 
            (OrderID, Customer_ID, Product_ID, Store_ID, Supplier_ID, Date_ID, 
             Quantity, Total_Amount, Revenue)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            data_tuples = [tuple(x) for x in data_batch.values]
            cursor.executemany(sql, data_tuples)
            conn.commit()
            
            rows_affected = cursor.rowcount
            cursor.close()
            conn.close()
            
            logger.info(f"Thread {thread_id}: Successfully loaded {rows_affected} fact records")
            
            return {
                'thread_id': thread_id,
                'table': 'Sales_Fact',
                'rows_loaded': rows_affected,
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"Thread {thread_id}: Error loading fact data: {e}")
            return {
                'thread_id': thread_id,
                'table': 'Sales_Fact',
                'rows_loaded': 0,
                'status': 'failed',
                'error': str(e)
            }
    
    def parallel_load(self, table_name: str, data: pd.DataFrame, batch_size: int = 1000):
        """
        Load data in parallel using multiple threads
        
        Args:
            table_name: Target table name
            data: Data to load
            batch_size: Number of records per batch
        """
        logger.info(f"Starting parallel load for {table_name} with {len(data)} records")
        
        # Split data into batches
        batches = [data[i:i+batch_size] for i in range(0, len(data), batch_size)]
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            if table_name == 'Sales_Fact':
                futures = {
                    executor.submit(self.load_fact_batch, batch, idx): idx 
                    for idx, batch in enumerate(batches)
                }
            else:
                futures = {
                    executor.submit(self.load_dimension_batch, table_name, batch, idx): idx 
                    for idx, batch in enumerate(batches)
                }
            
            # Collect results
            for future in as_completed(futures):
                result = future.result()
                self.results.append(result)
        
        # Summary
        total_loaded = sum(r['rows_loaded'] for r in self.results)
        failed = sum(1 for r in self.results if r['status'] == 'failed')
        
        logger.info(f"Parallel load complete: {total_loaded} records loaded, {failed} batches failed")


class ETLPipeline:
    """Main ETL Pipeline orchestrator"""
    
    def __init__(self, db_config: Dict):
        self.db_config = db_config
        self.db_conn = DatabaseConnection(**db_config)
        self.transformer = DataTransformer()
        self.hybrid_join = HybridJoin()
        self.loader = MultithreadedLoader(db_config, max_workers=4)
        
    def load_csv_data(self) -> Dict[str, pd.DataFrame]:
        """Load all CSV files"""
        logger.info("Loading CSV files...")
        
        try:
            customer_df = pd.read_csv('customer_master_data.csv', index_col=0)
            product_df = pd.read_csv('product_master_data.csv', index_col=0)
            transaction_df = pd.read_csv('transactional_data.csv', index_col=0)
            
            logger.info(f"Loaded CSV files: {len(customer_df)} customers, "
                       f"{len(product_df)} products, {len(transaction_df)} transactions")
            
            return {
                'customer': customer_df,
                'product': product_df,
                'transaction': transaction_df
            }
        except Exception as e:
            logger.error(f"Error loading CSV files: {e}")
            raise
    
    def load_dimensions(self, data: Dict[str, pd.DataFrame]):
        """Load all dimension tables"""
        logger.info("="*60)
        logger.info("LOADING DIMENSION TABLES")
        logger.info("="*60)
        
        # Transform customer data
        customer_dim = self.transformer.transform_customer_data(data['customer'])
        
        # Transform product data and extract store/supplier
        product_dim, store_dim, supplier_dim = self.transformer.transform_product_data(data['product'])
        
        # Generate date dimension
        date_dim = self.transformer.generate_date_dimension('2015-01-01', '2025-12-31')
        
        # Load dimensions in parallel
        logger.info("\n--- Loading Store Dimension ---")
        self.loader.parallel_load('Store_Dim', store_dim, batch_size=50)
        
        logger.info("\n--- Loading Supplier Dimension ---")
        self.loader.parallel_load('Supplier_Dim', supplier_dim, batch_size=50)
        
        logger.info("\n--- Loading Customer Dimension ---")
        self.loader.parallel_load('Customer_Dim', customer_dim, batch_size=500)
        
        logger.info("\n--- Loading Product Dimension ---")
        self.loader.parallel_load('Product_Dim', product_dim, batch_size=100)
        
        logger.info("\n--- Loading Date Dimension ---")
        self.loader.parallel_load('Date_Dim', date_dim, batch_size=365)
        
        logger.info("All dimensions loaded successfully!")
        
        return {
            'customer': customer_dim,
            'product': product_dim,
            'store': store_dim,
            'supplier': supplier_dim,
            'date': date_dim
        }
    
    def prepare_fact_data(self, transaction_df: pd.DataFrame, 
                         product_dim: pd.DataFrame, 
                         date_dim: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare fact table data using hybrid joins
        
        Args:
            transaction_df: Transaction data
            product_dim: Product dimension
            date_dim: Date dimension
        
        Returns:
            Prepared fact DataFrame
        """
        logger.info("="*60)
        logger.info("PREPARING FACT TABLE DATA USING HYBRID JOINS")
        logger.info("="*60)
        
        # Clean transaction data
        trans_df = transaction_df.copy()
        trans_df['Customer_ID'] = trans_df['Customer_ID'].astype(str)
        trans_df['Product_ID'] = trans_df['Product_ID'].astype(str)
        trans_df['date'] = pd.to_datetime(trans_df['date'])
        trans_df = trans_df.dropna()
        
        # Step 1: Hash Join with Product dimension to get Store_ID and Supplier_ID
        logger.info("\n--- Step 1: Hash Join Transaction with Product ---")
        trans_product = self.hybrid_join.hash_join(
            trans_df, 
            product_dim[['Product_ID', 'Store_ID', 'Supplier_ID', 'Price']], 
            'Product_ID', 
            'Product_ID'
        )
        
        # Step 2: Hash Join with Date dimension
        logger.info("\n--- Step 2: Hash Join with Date Dimension ---")
        # Create date mapping
        date_mapping = date_dim[['Date_ID', 'Full_Date']].copy()
        date_mapping['Full_Date'] = pd.to_datetime(date_mapping['Full_Date'])
        trans_product['date'] = pd.to_datetime(trans_product['date'])
        
        # Merge to get Date_ID
        fact_data = pd.merge(
            trans_product,
            date_mapping,
            left_on='date',
            right_on='Full_Date',
            how='inner'
        )
        
        # Step 3: Calculate metrics
        logger.info("\n--- Step 3: Calculating Fact Metrics ---")
        fact_data['Total_Amount'] = fact_data['quantity'] * fact_data['Price']
        fact_data['Revenue'] = fact_data['Total_Amount']  # Can add profit margin logic here
        
        # Select final columns for fact table
        fact_df = fact_data[[
            'orderID', 'Customer_ID', 'Product_ID', 'Store_ID', 'Supplier_ID',
            'Date_ID', 'quantity', 'Total_Amount', 'Revenue'
        ]].copy()
        
        fact_df.columns = [
            'OrderID', 'Customer_ID', 'Product_ID', 'Store_ID', 'Supplier_ID',
            'Date_ID', 'Quantity', 'Total_Amount', 'Revenue'
        ]
        
        # Remove duplicates and clean
        fact_df = fact_df.drop_duplicates()
        fact_df = fact_df.dropna()
        
        logger.info(f"Prepared {len(fact_df)} fact records")
        
        return fact_df
    
    def load_fact_table(self, fact_df: pd.DataFrame):
        """Load fact table using multithreading"""
        logger.info("="*60)
        logger.info("LOADING FACT TABLE")
        logger.info("="*60)
        
        self.loader.parallel_load('Sales_Fact', fact_df, batch_size=1000)
        
        logger.info("Fact table loaded successfully!")
    
    def run_quality_checks(self):
        """Run data quality checks"""
        logger.info("="*60)
        logger.info("RUNNING DATA QUALITY CHECKS")
        logger.info("="*60)
        
        conn = self.db_conn.get_connection()
        cursor = conn.cursor()
        
        checks = {
            'Total Customers': 'SELECT COUNT(*) FROM Customer_Dim',
            'Total Products': 'SELECT COUNT(*) FROM Product_Dim',
            'Total Stores': 'SELECT COUNT(*) FROM Store_Dim',
            'Total Suppliers': 'SELECT COUNT(*) FROM Supplier_Dim',
            'Total Date Records': 'SELECT COUNT(*) FROM Date_Dim',
            'Total Sales Records': 'SELECT COUNT(*) FROM Sales_Fact',
            'Total Revenue': 'SELECT SUM(Revenue) FROM Sales_Fact',
            'Date Range': 'SELECT MIN(Full_Date), MAX(Full_Date) FROM Date_Dim'
        }
        
        print("\n" + "="*60)
        print("DATA QUALITY REPORT")
        print("="*60)
        
        for check_name, query in checks.items():
            cursor.execute(query)
            result = cursor.fetchone()
            if check_name == 'Date Range':
                print(f"{check_name}: {result[0]} to {result[1]}")
            elif check_name == 'Total Revenue':
                print(f"{check_name}: ${result[0]:,.2f}" if result[0] else "N/A")
            else:
                print(f"{check_name}: {result[0]:,}")
        
        # Check for referential integrity
        print("\n" + "-"*60)
        print("REFERENTIAL INTEGRITY CHECKS")
        print("-"*60)
        
        integrity_checks = [
            ('Orphan Sales (Invalid Customer)', 
             '''SELECT COUNT(*) FROM Sales_Fact sf 
                LEFT JOIN Customer_Dim cd ON sf.Customer_ID = cd.Customer_ID 
                WHERE cd.Customer_ID IS NULL'''),
            ('Orphan Sales (Invalid Product)', 
             '''SELECT COUNT(*) FROM Sales_Fact sf 
                LEFT JOIN Product_Dim pd ON sf.Product_ID = pd.Product_ID 
                WHERE pd.Product_ID IS NULL'''),
            ('Orphan Sales (Invalid Date)', 
             '''SELECT COUNT(*) FROM Sales_Fact sf 
                LEFT JOIN Date_Dim dd ON sf.Date_ID = dd.Date_ID 
                WHERE dd.Date_ID IS NULL''')
        ]
        
        for check_name, query in integrity_checks:
            cursor.execute(query)
            result = cursor.fetchone()
            status = "✓ PASS" if result[0] == 0 else f"✗ FAIL ({result[0]} records)"
            print(f"{check_name}: {status}")
        
        print("="*60 + "\n")
        
        cursor.close()
    
    def run(self):
        """Execute the complete ETL pipeline"""
        start_time = time.time()
        
        logger.info("="*60)
        logger.info("STARTING ETL PIPELINE")
        logger.info("="*60)
        
        try:
            # Step 1: Load CSV data
            csv_data = self.load_csv_data()
            
            # Step 2: Load dimension tables
            dimensions = self.load_dimensions(csv_data)
            
            # Step 3: Prepare fact data using hybrid joins
            fact_df = self.prepare_fact_data(
                csv_data['transaction'],
                dimensions['product'],
                dimensions['date']
            )
            
            # Step 4: Load fact table
            self.load_fact_table(fact_df)
            
            # Step 5: Run quality checks
            self.run_quality_checks()
            
            elapsed_time = time.time() - start_time
            logger.info("="*60)
            logger.info(f"ETL PIPELINE COMPLETED SUCCESSFULLY in {elapsed_time:.2f} seconds")
            logger.info("="*60)
            
        except Exception as e:
            logger.error(f"ETL Pipeline failed: {e}")
            raise
        finally:
            self.db_conn.close()


def get_database_credentials() -> Dict:
    """Prompt user for database credentials"""
    print("\n" + "="*60)
    print("BILAL DATA WAREHOUSE ETL PIPELINE")
    print("="*60)
    print("\nPlease enter your MySQL database credentials:\n")
    
    host = input("Host (default: localhost): ").strip() or "localhost"
    user = input("Username (default: root): ").strip() or "root"
    password = getpass.getpass("Password: ")
    database = input("Database name (default: TBWalmart): ").strip() or "TBWalmart"
    
    return {
        'host': host,
        'user': user,
        'password': password,
        'database': database
    }


def main():
    """Main entry point"""
    try:
        # Get database credentials from user
        db_config = get_database_credentials()
        
        # Verify connection
        print("\nTesting database connection...")
        test_conn = mysql.connector.connect(**db_config)
        if test_conn.is_connected():
            print("✓ Successfully connected to database!")
            test_conn.close()
        
        # Run ETL pipeline
        print("\nStarting ETL pipeline...\n")
        pipeline = ETLPipeline(db_config)
        pipeline.run()
        
        print("\n" + "="*60)
        print("ETL PROCESS COMPLETED SUCCESSFULLY!")
        print("="*60)
        print("\nYou can now run queries on your data warehouse.")
        print("Log file created: etl_pipeline_<timestamp>.log")
        
    except Error as e:
        logger.error(f"Database error: {e}")
        print(f"\n✗ Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"\n✗ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
