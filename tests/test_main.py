
"""
Tests for the main module.
"""
import pytest
from pathlib import Path
import pandas as pd
from src.duckdb_project.main import DuckDBManager

def test_duckdb_manager_connection():
    """Test DuckDB connection."""
    with DuckDBManager() as db:
        assert db.conn is not None

def test_query_execution():
    """Test query execution."""
    with DuckDBManager() as db:
        result = db.execute_query("SELECT 42 as number")
        assert result.fetchone()[0] == 42

def test_invalid_query():
    """Test handling of invalid query."""
    with pytest.raises(Exception):
        with DuckDBManager() as db:
            db.execute_query("INVALID SQL")

def get_test_data_path():
    """Get path to test data directory."""
    return Path(__file__).parent / "fixtures" / "sample_sales.csv"

def test_load_csv_data():
    """Test loading CSV data into DuckDB."""
    with DuckDBManager() as db:
        csv_path = get_test_data_path()
        db.load_csv_data(csv_path, "sales")
        result = db.execute_query("SELECT COUNT(*) as count FROM sales")
        assert result.fetchone()[0] == 10

def test_sales_summary():
    """Test sales summary calculation."""
    with DuckDBManager() as db:
        csv_path = get_test_data_path()
        db.load_csv_data(csv_path, "sales")
        summary = db.get_sales_summary()
        df = summary.df()
        
        # Electronics should have the highest revenue
        assert df.iloc[0]['category'] == 'Electronics'
        assert len(df) == 3  # Electronics, Furniture, Appliances
