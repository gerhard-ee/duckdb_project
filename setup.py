from setuptools import setup, find_packages

setup(
    name="duckdb_project",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "duckdb>=0.9.0",
        "pandas>=2.0.0",
    ],
    python_requires=">=3.8",
)

