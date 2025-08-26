from __future__ import annotations
import snowflake.connector
from contextlib import contextmanager
from etl.config import settings

def connect():
    return snowflake.connector.connect(
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        warehouse=settings.snowflake_warehouse,
        database=settings.snowflake_database,
        schema=settings.snowflake_schema,
    )

@contextmanager
def cursor():
    con = connect()
    try:
        cur = con.cursor()
        yield cur
        con.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        con.close()
