# Moved from: fetch_sqlserver/fetch_sqldata.py
import json

import pandas as pd
import pyodbc


def extract_data(server,database,username,password,port):

    driver='ODBC Driver 17 for SQL Server'

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
            for sheetname,query in queries.items():
                df = pd.read_sql(query, conn)
                df.to_excel(writer, sheet_name=sheetname,index=False)

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
