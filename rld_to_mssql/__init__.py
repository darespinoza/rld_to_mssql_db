from dagster import (Definitions, 
                    FilesystemIOManager,
                    AssetSelection,
                    ScheduleDefinition,
                    define_asset_job,
                    load_assets_from_modules)
# from dagster_duckdb import DuckDBResource

# Import all assets from assets.py
from . import assets

all_assets = load_assets_from_modules([assets])

# Define a job that will materialize all assets
nrg_mssql_job = define_asset_job("nrg_mssql_job", selection=AssetSelection.all())

# ScheduleDefinition the job it should run and a cron schedule of how frequently to run it
nrg_mssql_schedule = ScheduleDefinition(
    job=nrg_mssql_job,
    cron_schedule="0 0 * * *",  # Every day at 00:00
)

# File system IO Manager
fs_io_manager = FilesystemIOManager(
    base_dir="iom_data",
)

# DuckDB IO Manager
# duckdb_resource = DuckDBResource(
#     database="duckdb_data/rld_mmsql.duckdb",
# )

defs = Definitions(
    assets=all_assets,
    schedules=[nrg_mssql_schedule],
    resources={
        "fs_io_manager": fs_io_manager,
        # "duckdb_resource": duckdb_resource,
    },
)