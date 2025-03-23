    def __init__(self, db_path: str = ":memory:", use_motherduck: bool = False):
        """
        Initialize DuckDB connection.
        
        Args:
            db_path (str): Path to the database file, ":memory:" for in-memory database,
                         or motherduck:// URL for MotherDuck
            use_motherduck (bool): Whether to use MotherDuck
        """
        self.use_motherduck = use_motherduck
        if use_motherduck:
            # Get token from environment variable
            self.token = os.getenv('MOTHERDUCK_TOKEN')
            if not self.token:
                raise ValueError("MOTHERDUCK_TOKEN environment variable not set")
            self.db_path = db_path if db_path != ":memory:" else "md:"
        else:
            self.db_path = db_path
        self.conn = None
    def connect(self):
        """Establish connection to DuckDB."""
        try:
            if self.use_motherduck:
                self.conn = duckdb.connect(self.db_path, config={'motherduck_token': self.token})
            else:
                self.conn = duckdb.connect(self.db_path)
            logger.info("Successfully connected to DuckDB at %s", self.db_path)
        except Exception as e:
            logger.error("Failed to connect to DuckDB: %s", str(e))
            raise
        return self.execute_query(query)
        
    def upload_to_motherduck(self, local_table: str, motherduck_db: str, motherduck_table: str) -> None:
        """
        Upload a local table to MotherDuck.
        
        Args:
            local_table (str): Name of the local table to upload
            motherduck_db (str): MotherDuck database name
            motherduck_table (str): Name for the table in MotherDuck
        """
        if not self.use_motherduck:
            raise ValueError("MotherDuck connection not configured")
        
        try:
            # Create the target database if it doesn't exist
            self.execute_query(f"CREATE DATABASE IF NOT EXISTS {motherduck_db}")
            
            # Create the table in MotherDuck and copy the data
            copy_query = f"""
                CREATE TABLE IF NOT EXISTS {motherduck_db}.{motherduck_table} AS 
                SELECT * FROM {local_table}
            """
            self.execute_query(copy_query)
            logger.info("Successfully uploaded %s to MotherDuck as %s.%s", 
                      local_table, motherduck_db, motherduck_table)
        except Exception as e:
            logger.error("Failed to upload to MotherDuck: %s", str(e))
            raise

def main():
    # Get the path to the sample data
    from pathlib import Path
    sample_data_path = Path(__file__).parent.parent.parent / "tests" / "fixtures" / "sample_sales.csv"
    
    # Check if we have a MotherDuck token
    use_motherduck = bool(os.getenv('MOTHERDUCK_TOKEN'))
    
    with DuckDBManager(use_motherduck=use_motherduck) as db:
            print(f"  Total Transactions: {row['total_transactions']}")
            print(f"  Total Items Sold: {row['total_items_sold']}")
            print(f"  Total Revenue: ${row['total_revenue']:,.2f}")
        
        # Upload to MotherDuck if configured
        if use_motherduck:
            db.upload_to_motherduck("sales", "demo_db", "sales_data")

if __name__ == "__main__":
