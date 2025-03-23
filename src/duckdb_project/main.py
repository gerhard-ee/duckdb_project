
"""
Main module for the DuckDB project.
"""
import os
import logging
from pathlib import Path
import duckdb

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DuckDBManager:
    """Manager class for DuckDB operations."""
    
    def __init__(self, db_path: str = ":memory:", use_motherduck: bool = False):
        """
        Initialize DuckDB connection.
        
        Args:
            db_path (str): Path to the database file or motherduck:// URL
            use_motherduck (bool): Whether to use MotherDuck
        """
        self.use_motherduck = use_motherduck
        if use_motherduck:
            self.token = os.getenv('MOTHERDUCK_TOKEN')
            if not self.token:
                raise ValueError("MOTHERDUCK_TOKEN environment variable not set")
            # Use database name without protocol
            self.db_path = "demo_db"
        else:
            self.db_path = db_path
        self.conn = None
        
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        
    def connect(self):
        """Establish connection to DuckDB."""
        try:
            if self.use_motherduck:
                # First connect to MotherDuck without specific database
                md_url = f"md:?motherduck_token={self.token}"
                self.conn = duckdb.connect(md_url)
                
                # Create the database if it doesn't exist
                self.conn.sql(f"CREATE DATABASE IF NOT EXISTS {self.db_path}")
                
                # Now attach to the specific database
                self.conn.sql(f"USE {self.db_path}")
                
                logger.info("Successfully connected to MotherDuck database: %s", self.db_path)
            else:
                self.conn = duckdb.connect(self.db_path)
                logger.info("Successfully connected to local DuckDB at %s", self.db_path)
        except Exception as e:
            logger.error("Failed to connect to DuckDB: %s", str(e))
            raise
            
    def close(self):
        """Close the DuckDB connection."""
        if self.conn:
            self.conn.close()
            logger.info("Closed DuckDB connection")
            
    def execute_query(self, query: str) -> duckdb.DuckDBPyRelation:
        """
        Execute a SQL query.
        
        Args:
            query (str): SQL query to execute
            
        Returns:
            duckdb.DuckDBPyRelation: Query result
        """
        try:
            result = self.conn.sql(query)
            logger.debug("Successfully executed query: %s", query)
            return result
        except Exception as e:
            logger.error("Failed to execute query: %s", str(e))
            raise

    def load_csv_data(self, csv_path: str, table_name: str) -> None:
        """
        Load CSV data into a DuckDB table.
        
        Args:
            csv_path (str): Path to the CSV file
            table_name (str): Name of the table to create
        """
        try:
            query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path}')"
            self.execute_query(query)
            logger.info("Successfully loaded data from %s into table %s", csv_path, table_name)
        except Exception as e:
            logger.error("Failed to load CSV data: %s", str(e))
            raise

    def get_sales_summary(self) -> duckdb.DuckDBPyRelation:
        """
        Get a summary of sales data.
        
        Returns:
            duckdb.DuckDBPyRelation: Summary of sales by category
        """
        query = """
            SELECT 
                category,
                COUNT(*) as total_transactions,
                SUM(quantity) as total_items_sold,
                SUM(price * quantity) as total_revenue
            FROM sales
            GROUP BY category
            ORDER BY total_revenue DESC
        """
        return self.execute_query(query)

    def upload_to_motherduck(self, local_table: str, motherduck_table: str) -> None:
        """
        Upload a local table to MotherDuck.
        
        Args:
            local_table (str): Name of the local table to upload
            motherduck_table (str): Name for the table in MotherDuck
        """
        if not self.use_motherduck:
            raise ValueError("MotherDuck connection not configured")
        
        try:
            # Create the table in MotherDuck and copy the data
            copy_query = f"""
                CREATE OR REPLACE TABLE {motherduck_table} AS 
                SELECT * FROM {local_table}
            """
            self.execute_query(copy_query)
            logger.info("Successfully uploaded data to table: %s", motherduck_table)
        except Exception as e:
            logger.error("Failed to upload to MotherDuck: %s", str(e))
            raise

def main():
    """Main function demonstration."""
    # Get the path to the sample data
    sample_data_path = Path(__file__).parent.parent.parent / "tests" / "fixtures" / "sample_sales.csv"
    
    # Check if we have a MotherDuck token
    use_motherduck = bool(os.getenv('MOTHERDUCK_TOKEN'))
    
    # Initialize with MotherDuck support if token is present
    with DuckDBManager(use_motherduck=use_motherduck) as db:
        # Load sample sales data
        db.load_csv_data(sample_data_path, "sales")
        
        # Get and display sales summary
        print("\nSales Summary by Category:")
        print("------------------------")
        summary = db.get_sales_summary()
        result_df = summary.df()
        
        # Format the output for better readability
        for _, row in result_df.iterrows():
            print(f"\nCategory: {row['category']}")
            print(f"  Total Transactions: {row['total_transactions']}")
            print(f"  Total Items Sold: {row['total_items_sold']}")
            print(f"  Total Revenue: ${row['total_revenue']:,.2f}")
        
        # Upload to MotherDuck if configured
        if use_motherduck:
            print("\nUploading data to MotherDuck...")
            db.upload_to_motherduck("sales", "sales_data")
            print("Data upload complete!")

if __name__ == "__main__":
    main()

