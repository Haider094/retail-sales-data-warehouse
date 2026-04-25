"""
===============================================================================
Bronze Layer Loader: CSV -> SQL Server (Python Alternative to BULK INSERT)
===============================================================================
Author : Wajahat Haider
Purpose: Load ERP and CRM CSV source files into the Bronze schema using
         pandas for pre-load validation (null counts, duplicate detection)
         and pyodbc for SQL Server insertion.

Usage:
    python scripts/bronze/load_bronze.py

Prerequisites:
    pip install pandas pyodbc
    SQL Server ODBC Driver 17 (or 18) installed
===============================================================================
"""

import os
import time
import pyodbc
import pandas as pd

# ---------------------------------------------------------------------------
# Connection — adjust SERVER and DATABASE to match your environment
# ---------------------------------------------------------------------------
SERVER   = r"localhost\SQLEXPRESS"   # e.g. "localhost" or ".\SQLEXPRESS"
DATABASE = "DataWarehouse"

CONN_STR = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
)

# ---------------------------------------------------------------------------
# Dataset paths relative to repo root
# ---------------------------------------------------------------------------
BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "datasets")

FILES = {
    "bronze.crm_cust_info":    os.path.join(BASE_DIR, "source_crm", "cust_info.csv"),
    "bronze.crm_prd_info":     os.path.join(BASE_DIR, "source_crm", "prd_info.csv"),
    "bronze.crm_sales_details":os.path.join(BASE_DIR, "source_crm", "sales_details.csv"),
    "bronze.erp_cust_az12":    os.path.join(BASE_DIR, "source_erp", "CUST_AZ12.csv"),
    "bronze.erp_loc_a101":     os.path.join(BASE_DIR, "source_erp", "LOC_A101.csv"),
    "bronze.erp_px_cat_g1v2":  os.path.join(BASE_DIR, "source_erp", "PX_CAT_G1V2.csv"),
}


def validate(df: pd.DataFrame, table: str) -> None:
    """Log basic data quality stats before inserting."""
    total      = len(df)
    null_count = df.isnull().sum().sum()
    dup_count  = df.duplicated().sum()
    print(f"   Rows      : {total:,}")
    print(f"   Nulls     : {null_count:,}  ({null_count/max(total*len(df.columns),1)*100:.1f}% of cells)")
    print(f"   Duplicates: {dup_count:,}")


def load_table(cursor, table: str, df: pd.DataFrame) -> None:
    schema, tbl = table.split(".")
    cols   = ", ".join(df.columns)
    params = ", ".join(["?" for _ in df.columns])
    sql    = f"INSERT INTO {schema}.{tbl} ({cols}) VALUES ({params})"

    # Truncate first (mirrors BULK INSERT behaviour)
    cursor.execute(f"TRUNCATE TABLE {schema}.{tbl}")

    rows = [tuple(
        None if (isinstance(v, float) and pd.isna(v)) else v
        for v in row
    ) for row in df.itertuples(index=False, name=None)]

    cursor.executemany(sql, rows)


def main() -> None:
    print("=" * 56)
    print("Bronze Layer Load  —  Python Ingestion Script")
    print("=" * 56)

    conn = pyodbc.connect(CONN_STR)
    conn.autocommit = False
    cursor = conn.cursor()

    batch_start = time.time()

    for table, filepath in FILES.items():
        print(f"\n>> Loading {table}")
        t0 = time.time()

        df = pd.read_csv(filepath, low_memory=False)
        validate(df, table)

        load_table(cursor, table, df)
        conn.commit()

        elapsed = time.time() - t0
        print(f"   Duration  : {elapsed:.1f}s  ✓")

    total_elapsed = time.time() - batch_start
    cursor.close()
    conn.close()

    print("\n" + "=" * 56)
    print(f"Bronze load complete  —  {total_elapsed:.1f}s total")
    print("=" * 56)


if __name__ == "__main__":
    main()
