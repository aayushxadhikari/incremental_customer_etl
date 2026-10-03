"""Native MySQL connections for operations Spark JDBC can't express (UPDATEs)."""

from contextlib import contextmanager

import mysql.connector

from config.settings import Settings


@contextmanager
def get_connection(settings: Settings):
    """Yield a MySQL connection, committing on success and always closing."""
    conn = mysql.connector.connect(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        database=settings.mysql_db,
        connection_timeout=10,
        time_zone="+00:00",
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
