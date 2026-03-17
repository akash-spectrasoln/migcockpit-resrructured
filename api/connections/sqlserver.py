# Moved from: api/services/sqlserver_connector.py
"""
SQL Server connector for extracting data from SQL Server databases.
"""
import json

import pyodbc

# Lazy import pandas to avoid DLL load errors during Django startup
# Import only when the function is called, not at module level

def extract_data(server, database, username, password, port):
    """
    Extract data from SQL Server database.

    Args:
        server: SQL Server hostname
        database: Database name
        username: Username for connection
        password: Password for connection
        port: Port number

    Returns:
        Tuple of (success: bool, message: str)
    """
    # Lazy import pandas - only load when function is called
    import pandas as pd

    driver = 'ODBC Driver 17 for SQL Server'

    try:
        conn = pyodbc.connect(
            f'Driver={driver};'
            f'Server={server},{port};'
            f'Database={database};'
            f'UID={username};'
            f'PWD={password};'
        )
        conn.cursor()

        queries = {
            "MARA": "SELECT TOP 10 * FROM [erp].[MARA];",
            "MAKT": "SELECT TOP 10 * FROM [erp].[MAKT];",
            "MARD": "SELECT TOP 10 * FROM [erp].[MARD];",
            "MARM": "SELECT TOP 10 * FROM [erp].[MARM];"
        }

        all_metadata = {}  # Dictionary to collect metadata
        output_json = "table_metadata.json"
        output_file = "sql_results.xlsx"
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            for sheetname, query in queries.items():
                df = pd.read_sql(query, conn)
                df.to_excel(writer, sheet_name=sheetname, index=False)

                # Fetch metadata from INFORMATION_SCHEMA
                meta_query = """
                SELECT
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{sheetname}'
                """
                meta_df = pd.read_sql(meta_query, conn)
                all_metadata[sheetname] = meta_df.to_dict(orient='records')

        # Save metadata as JSON
        with open(output_json, 'w', encoding='utf-8') as f:
            json.dump(all_metadata, f, indent=4)

        print("finished")
        return True, "finished"

    except Exception as e:
        print(e)
        return False, str(e)

    finally:
        try:
            conn.close()
        except Exception:
            pass
    return False, "unknown error"
