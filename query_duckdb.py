import duckdb

db_file = "/home/nugget/workspace/nasa-port/pipeline_output/recent_discoveries.duckdb"

try:
    con = duckdb.connect(database=db_file, read_only=True)
    tables = con.execute("SELECT table_schema, table_name FROM information_schema.tables;").fetchall()
    print("Tables in the database (schema, table):")
    for table in tables:
        print(table)
    
    if tables:
        first_table_info = tables[0]
        schema_name = first_table_info[0]
        table_name = first_table_info[1]
        print(f"\nData from table '{schema_name}.{table_name}':")
        data = con.execute(f"SELECT * FROM {schema_name}.{table_name} LIMIT 10;").fetchall()
        for row in data:
            print(row)

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    if 'con' in locals():
        con.close()
