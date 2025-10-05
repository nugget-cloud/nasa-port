#!/usr/bin/env python3
"""
A simple, runnable example of how to execute a NASA Port data pipeline.

This script demonstrates the three core steps:
1. Import NASAPipeline.
2. Create a pipeline instance.
3. Call a `load_*` method to run the pipeline.
"""

import sys
from pathlib import Path
import duckdb

# --- 1. Setup Environment ---
# Add the 'src' directory to the Python path to allow imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.nasa_port.data_bindings import NASAPipeline
from src.nasa_port.builder.query_builder import QueryBuilder
from src.nasa_port.builder.models import TableName

def run_simple_pipeline():
    """
    This function shows the complete process of running a pipeline
    and inspecting the resulting database.
    """
    print("🚀 Starting a simple pipeline execution...")
    print("="*50)

    # --- 2. Create a Pipeline Instance ---
    # Use the helper for local development. This will create a DuckDB
    # database in the 'pipeline_output' directory.
    db_dir = "pipeline_output"
    db_name = "recent_discoveries.duckdb"
    db_path = Path(db_dir) / db_name

    # Ensure the output directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"🔧 Configuring pipeline to save data to: {db_path}")
    pipeline = NASAPipeline.for_local_development(
        pipeline_name="recent_discoveries_pipeline",
        data_dir=db_dir
    )
    # We will use a different name for the database file
    pipeline.config.destination_params["database_path"] = str(db_path)


    # --- 3. Define a Query and Call a `load_*` Method ---
    # We'll create a query to get the 10 most recently discovered planets.
    print("🛰️  Creating a query for the 10 most recent exoplanet discoveries...")
    recent_planets_query = (
        QueryBuilder()
        .select(['pl_name', 'hostname', 'discoverymethod', 'disc_year', 'sy_dist'])
        .from_table(TableName.PLANETARY_SYSTEMS)
        .where_confirmed()
        .order_by('disc_year', ascending=False)
        .limit(10)
    )

    print("⏳ Running the pipeline to load data... (This may take a moment)")
    try:
        # Execute the pipeline with our custom query
        load_info = pipeline.load_custom_query(
            query=recent_planets_query,
            resource_name="recent_discoveries"
        )
        print("✅ Pipeline executed successfully!")
        print(f"   Load packages processed: {len(load_info.load_packages)}")

    except Exception as e:
        print(f"❌ Pipeline execution failed: {e}")
        return

    # --- 4. Inspect the Created Database ---
    print("\n" + "="*50)
    print(f"🔎 Inspecting the created database: {db_path}")

    if not db_path.exists():
        print(f"❌ Database file was not created at {db_path}")
        return

    try:
        # Connect to the DuckDB database
        con = duckdb.connect(database=str(db_path), read_only=True)

        # List tables in the database
        tables = con.execute("SHOW TABLES").fetchall()
        print("📊 Tables in the database:")
        for table in tables:
            print(f"   - {table[0]}")

        # Query the data from the first table found
        if tables:
            table_name = tables[0][0]
            print(f"\n📋 Querying the first 10 rows from the '{table_name}' table:")
            results = con.execute(f"SELECT pl_name, hostname, disc_year FROM {table_name} LIMIT 10").fetchall()

            # Print results in a formatted way
            print(f"{'Planet Name':<20} | {'Host Star':<20} | {'Discovery Year':<15}")
            print("-" * 60)
            for row in results:
                print(f"{row[0]:<20} | {row[1]:<20} | {row[2]:<15}")

        con.close()
    except Exception as e:
        print(f"❌ Failed to inspect the database: {e}")


if __name__ == "__main__":
    run_simple_pipeline()
    print("\n🎉 Example finished.")