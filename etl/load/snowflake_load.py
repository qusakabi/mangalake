from __future__ import annotations
import datetime as dt
from typing import List
from uuid import uuid4

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

from etl.clients.snowflake_client import connect, cursor
from etl.config import settings

QUAL_DB = settings.snowflake_database
QUAL_SCHEMA = settings.snowflake_schema

def _q(name: str) -> str:
    return name  # relying on uppercase defaults

def ensure_snowflake_objects() -> None:
    with cursor() as cur:
        # Warehouse/DB/Schema (idempotent)
        cur.execute(f"CREATE WAREHOUSE IF NOT EXISTS {_q(settings.snowflake_warehouse)} WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE")
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {_q(settings.snowflake_database)}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_q(settings.snowflake_schema)}")
        # ODS
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_q(QUAL_DB)}.{_q(QUAL_SCHEMA)}.ODS_MANGA (
          MANGA_ID STRING,
          TITLE STRING,
          STATUS STRING,
          LAST_CHAPTER STRING,
          YEAR NUMBER(4,0),
          TAGS STRING,
          UPDATED_AT TIMESTAMP_NTZ,
          LOAD_DATE DATE
        )
        """)
        # DM
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_q(QUAL_DB)}.{_q(QUAL_SCHEMA)}.DM_MANGA_SUMMARY (
          LOAD_DATE DATE,
          YEAR NUMBER(4,0),
          STATUS STRING,
          COUNT_MANGA NUMBER
        )
        """)
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_q(QUAL_DB)}.{_q(QUAL_SCHEMA)}.DM_MANGA_DAILY_COUNTS (
          LOAD_DATE DATE,
          STATUS STRING,
          COUNT_MANGA NUMBER
        )
        """)
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_q(QUAL_DB)}.{_q(QUAL_SCHEMA)}.DM_MANGA_AVG_YEAR (
          LOAD_DATE DATE,
          STATUS STRING,
          AVG_YEAR NUMBER(10,2)
        )
        """)

def _prepare_df(df: pd.DataFrame, ds: str) -> pd.DataFrame:
    df = df.copy()
    # ds is a single date string; assign scalar date to the whole column
    df["LOAD_DATE"] = pd.to_datetime(ds).date()
    # Normalize timestamp-like columns if present
    if "UPDATED_AT" in df.columns:
        df["UPDATED_AT"] = pd.to_datetime(df["UPDATED_AT"], errors="coerce")
    # Coerce types
    if "YEAR" in df.columns:
        df["YEAR"] = pd.to_numeric(df["YEAR"], errors="coerce").astype("Int64")
    return df

def load_ods_manga(df: pd.DataFrame, ds: str) -> None:
    df2 = _prepare_df(df, ds)
    con = connect()
    temp_table = f"TEMP_ODS_MANGA_{uuid4().hex[:8]}"
    try:
        with con.cursor() as cur:
            # Set execution context and ensure target table exists
            cur.execute(f"USE DATABASE {QUAL_DB}")
            cur.execute(f"USE SCHEMA {QUAL_SCHEMA}")
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS ODS_MANGA (
              MANGA_ID STRING,
              TITLE STRING,
              STATUS STRING,
              LAST_CHAPTER STRING,
              YEAR NUMBER(4,0),
              TAGS STRING,
              UPDATED_AT TIMESTAMP_NTZ,
              LOAD_DATE DATE
            )
            """)
            # Create a session TEMP table with the same structure for staging
            cur.execute(f"CREATE TEMP TABLE {temp_table} LIKE ODS_MANGA")

        # Insert DataFrame rows into the session TEMP table
        # Avoid auto_create_table; write into the existing TEMP table
        success, nchunks, nrows, _ = write_pandas(
            con,
            df2,
            temp_table,
            auto_create_table=False,
            overwrite=True,
            use_logical_type=True,
        )
        if not success or nrows == 0:
            raise RuntimeError(f"write_pandas wrote no rows (success={success}, nrows={nrows})")

        with con.cursor() as cur:
            cur.execute(f"""
            MERGE INTO ODS_MANGA AS T
            USING {temp_table} AS S
              ON T.MANGA_ID = S.MANGA_ID AND T.LOAD_DATE = S.LOAD_DATE
            WHEN MATCHED THEN UPDATE SET
              TITLE = S.TITLE,
              STATUS = S.STATUS,
              LAST_CHAPTER = S.LAST_CHAPTER,
              YEAR = S.YEAR,
              TAGS = S.TAGS,
              UPDATED_AT = S.UPDATED_AT
            WHEN NOT MATCHED THEN INSERT (MANGA_ID, TITLE, STATUS, LAST_CHAPTER, YEAR, TAGS, UPDATED_AT, LOAD_DATE)
            VALUES (S.MANGA_ID, S.TITLE, S.STATUS, S.LAST_CHAPTER, S.YEAR, S.TAGS, S.UPDATED_AT, S.LOAD_DATE)
            """)
            cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
        con.commit()
    finally:
        con.close()

def build_dm_manga_summary(ds: str) -> None:
    with cursor() as cur:
        cur.execute(f"DELETE FROM {QUAL_DB}.{QUAL_SCHEMA}.DM_MANGA_SUMMARY WHERE LOAD_DATE = TO_DATE(%s)", (ds,))
        cur.execute(f"""
        INSERT INTO {QUAL_DB}.{QUAL_SCHEMA}.DM_MANGA_SUMMARY (LOAD_DATE, YEAR, STATUS, COUNT_MANGA)
        SELECT TO_DATE(%s) AS LOAD_DATE, YEAR, STATUS, COUNT(*)
          FROM {QUAL_DB}.{QUAL_SCHEMA}.ODS_MANGA
         WHERE LOAD_DATE = TO_DATE(%s)
         GROUP BY YEAR, STATUS
        """, (ds, ds))

def build_dm_manga_daily_counts(ds: str) -> None:
    with cursor() as cur:
        cur.execute(f"DELETE FROM {QUAL_DB}.{QUAL_SCHEMA}.DM_MANGA_DAILY_COUNTS WHERE LOAD_DATE = TO_DATE(%s)", (ds,))
        cur.execute(f"""
        INSERT INTO {QUAL_DB}.{QUAL_SCHEMA}.DM_MANGA_DAILY_COUNTS (LOAD_DATE, STATUS, COUNT_MANGA)
        SELECT TO_DATE(%s) AS LOAD_DATE, STATUS, COUNT(*)
          FROM {QUAL_DB}.{QUAL_SCHEMA}.ODS_MANGA
         WHERE LOAD_DATE = TO_DATE(%s)
         GROUP BY STATUS
        """, (ds, ds))

def build_dm_manga_avg_year(ds: str) -> None:
    with cursor() as cur:
        cur.execute(f"DELETE FROM {QUAL_DB}.{QUAL_SCHEMA}.DM_MANGA_AVG_YEAR WHERE LOAD_DATE = TO_DATE(%s)", (ds,))
        cur.execute(f"""
        INSERT INTO {QUAL_DB}.{QUAL_SCHEMA}.DM_MANGA_AVG_YEAR (LOAD_DATE, STATUS, AVG_YEAR)
        SELECT TO_DATE(%s) AS LOAD_DATE, STATUS, AVG(NULLIF(YEAR, 0))
          FROM {QUAL_DB}.{QUAL_SCHEMA}.ODS_MANGA
         WHERE LOAD_DATE = TO_DATE(%s)
         GROUP BY STATUS
        """, (ds, ds))

def build_dm_all(ds: str) -> None:
    """
    Convenience wrapper to build all DM tables for a given ds in one call.
    Executes:
      - DM_MANGA_SUMMARY
      - DM_MANGA_DAILY_COUNTS
      - DM_MANGA_AVG_YEAR
    """
    build_dm_manga_summary(ds)
    build_dm_manga_daily_counts(ds)
    build_dm_manga_avg_year(ds)
