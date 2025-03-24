
# DuckDB Python Project

This project demonstrates the usage of DuckDB with Python for efficient data processing and analytics.

## Project Structure

```
duckdb_project/
├── config/         # Configuration files
├── data/          # Data files
├── docs/          # Documentation
├── src/           # Source code
│   └── duckdb_project/
│       ├── __init__.py
│       └── main.py
└── tests/         # Test files
    ├── __init__.py
    └── test_main.py
```

## AI Rules

This project follows these AI-driven development principles:

1. **Security First**: All code is written with security best practices, including input validation, proper error handling, and protection against common vulnerabilities.

2. **Production-Ready**: Code is designed to be reliable, performant, and maintainable in production environments.

3. **Modularity**: The codebase is organized in a modular fashion, with clear separation of concerns and well-defined interfaces.

4. **Senior-Level Engineering**: Implementation follows senior-level engineering practices, including comprehensive documentation, thoughtful architecture, and clean code principles.

5. **Testing**: All code has appropriate test coverage with unit, integration, and where applicable, end-to-end tests.

6. **Documentation**: Code is well-documented with clear explanations of functionality, usage examples, and API documentation.

## Setup

1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: .\venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

DuckDB is an in-process SQL OLAP database management system designed for analytical queries. Here are some examples of how to use this project:

### Basic Query Execution

```python
from duckdb_project.main import create_connection, execute_query

# Create a DuckDB connection
conn = create_connection()

# Execute a simple query
result = execute_query(conn, "SELECT 42 AS answer")
print(result)
```

### Working with CSV Data

```python
from duckdb_project.main import load_csv, query_csv_data

# Load a CSV file into DuckDB
csv_path = "data/example.csv"
table_name = "my_data"
load_csv(conn, csv_path, table_name)

# Query the loaded data
query = f"""
SELECT 
    column1, 
    AVG(numeric_column) as avg_value
FROM {table_name}
GROUP BY column1
ORDER BY avg_value DESC
"""
results = execute_query(conn, query)
print(results)
```

### Data Transformation Pipeline

```python
from duckdb_project.main import run_data_pipeline

# Define pipeline steps
pipeline_config = {
    "input_file": "data/raw_data.csv",
    "output_file": "data/processed_data.parquet",
    "transformations": [
        "SELECT * FROM input WHERE value > 0",
        "SELECT id, SUM(value) as total FROM filtered GROUP BY id"
    ]
}

# Run the pipeline
run_data_pipeline(pipeline_config)
```

### Integration with Pandas

```python
import pandas as pd
from duckdb_project.main import pandas_to_duckdb, query_pandas_df

# Create a pandas DataFrame
df = pd.DataFrame({
    'A': range(1, 11),
    'B': range(11, 21)
})

# Register the DataFrame with DuckDB
pandas_to_duckdb(conn, df, "pandas_table")

# Query the DataFrame using SQL
result = execute_query(conn, "SELECT SUM(A) as sum_a, AVG(B) as avg_b FROM pandas_table")
print(result)
```

### Performance Benchmarking

```python
from duckdb_project.main import benchmark_query

# Run a performance benchmark comparing DuckDB with Pandas
query = "SELECT col1, SUM(col2) FROM large_table GROUP BY col1"
benchmark_results = benchmark_query(query, ["duckdb", "pandas"])
print(benchmark_results)
```

## MotherDuck Integration

[MotherDuck](https://motherduck.com/) is a managed cloud service for DuckDB that enables you to scale beyond memory limits and collaborate on DuckDB databases. It combines the simplicity of DuckDB with the scalability of the cloud.

### Obtaining a MotherDuck Access Token

To connect to MotherDuck, you'll need an access token:

1. Sign up for a MotherDuck account at [motherduck.com](https://motherduck.com/)
2. Log in to your account dashboard
3. Navigate to the "Access Tokens" section
4. Click "Generate New Token" and follow the instructions
5. Copy your newly generated token for later use

### Securely Setting Up Your Token

For security reasons, never hardcode your MotherDuck token in your source code. Instead, use environment variables:

#### Setting up the environment variable:

**Linux/macOS:**
```bash
# Add to your ~/.bashrc, ~/.zshrc, or equivalent
export MOTHERDUCK_TOKEN="your_token_here"
```

**Windows (Command Prompt):**
```cmd
setx MOTHERDUCK_TOKEN "your_token_here"
```

**Windows (PowerShell):**
```powershell
[Environment]::SetEnvironmentVariable("MOTHERDUCK_TOKEN", "your_token_here", "User")
```

### Connecting to MotherDuck

Here's how to use the environment variable to connect to MotherDuck securely:

```python
import os
import duckdb

def create_motherduck_connection():
    """Create a secure connection to MotherDuck using environment variable."""
    token = os.environ.get("MOTHERDUCK_TOKEN")
    
    if not token:
        raise EnvironmentError(
            "MOTHERDUCK_TOKEN environment variable not found. "
            "Please set it with your MotherDuck access token."
        )
    
    # Connect to MotherDuck using the token
    conn = duckdb.connect("md:?motherduck_token=" + token)
    return conn

# Example usage
try:
    # Create a secure connection
    md_conn = create_motherduck_connection()
    
    # Execute a query on MotherDuck
    result = md_conn.execute("SELECT 'Connected to MotherDuck!' AS status").fetchall()
    print(result[0][0])
    
    # Load and query data
    md_conn.execute("CREATE OR REPLACE TABLE sample_data AS SELECT * FROM range(1000) AS x")
    result = md_conn.execute("SELECT COUNT(*) FROM sample_data").fetchall()
    print(f"Count: {result[0][0]}")
    
except Exception as e:
    print(f"Error: {e}")
```

### Security Best Practices

When working with MotherDuck and access tokens:

1. **Never commit tokens to version control**. Use `.gitignore` to exclude any files that might contain tokens.

2. **Rotate your tokens periodically** according to your organization's security policies.

3. **Use different tokens for development and production** environments.

4. **Set appropriate permissions** for your tokens in the MotherDuck dashboard.

5. **Consider using a secrets management solution** like HashiCorp Vault, AWS Secrets Manager, or similar tools in production environments.

6. **Implement token validation** in your application startup to ensure the token is properly configured.

7. **In containerized environments**, use the container platform's secrets management (Docker secrets, Kubernetes secrets, etc.).

## Testing

Run tests using pytest:
```bash
pytest tests/
```

## License

MIT

