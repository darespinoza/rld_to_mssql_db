from setuptools import find_packages, setup

setup(
    name="rld_to_mssql",
    packages=find_packages(exclude=["rld_to_mssql_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "duckdb",
        "sqlalchemy",
        "pyodbc",
        "python-dotenv",
        "nrgpy",
        "pymssql"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)