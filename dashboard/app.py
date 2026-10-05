"""Browser dashboard for the customer ETL database (read-only queries)."""
import os
from pathlib import Path

import mysql.connector
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
st.set_page_config(page_title="Customer ETL", page_icon="📊", layout="wide")


def connect():
    load_dotenv(ROOT / "config/db.env")
    required = ("MYSQL_HOST", "MYSQL_DB", "MYSQL_USER", "MYSQL_PASSWORD")
    if any(not os.getenv(key) for key in required):
        raise ValueError("Configure MYSQL_HOST, MYSQL_DB, MYSQL_USER and MYSQL_PASSWORD in config/db.env.")
    port = int(os.getenv("MYSQL_PORT", "3307"))
    if not 1 <= port <= 65535:
        raise ValueError("MYSQL_PORT must be between 1 and 65535.")
    return mysql.connector.connect(
        host=os.environ["MYSQL_HOST"], port=port,
        database=os.environ["MYSQL_DB"], user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"], connection_timeout=5,
        time_zone="+00:00",
    )


def query(conn, sql, params=()):
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        return pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)


def show_table(frame):
    if frame.empty:
        st.info("No matching records yet.")
    else:
        st.dataframe(frame, hide_index=True, use_container_width=True)


def render(conn):
    totals = query(conn, """SELECT
        (SELECT COUNT(*) FROM customer_master WHERE active_flag=1) AS customers,
        (SELECT COUNT(*) FROM customer_master WHERE active_flag=0) AS historical,
        (SELECT COUNT(*) FROM ingestion_batches WHERE status='READY') AS pending,
        (SELECT COUNT(*) FROM customer_rejects) AS rejected""").iloc[0]
    for col, label, key in zip(st.columns(4),
                             ["Current customers", "Historical versions", "Pending batches", "Rejected rows"],
                             ["customers", "historical", "pending", "rejected"]):
        col.metric(label, f"{int(totals[key]):,}")

    overview, customers, history, rejects = st.tabs(
        ["Pipeline overview", "Current customers", "Customer history", "Data quality"]
    )
    with overview:
        st.subheader("Recent pipeline runs")
        runs = query(conn, """SELECT run_id,batch_id,status,started_at,finished_at,
            TIMESTAMPDIFF(SECOND,started_at,finished_at) AS duration_seconds,
            extracted,valid_rows,inserts,updates,rejects,message
            FROM etl_run_log ORDER BY run_id DESC LIMIT 100""")
        if not runs.empty:
            st.bar_chart(runs.sort_values("run_id").set_index("run_id")[["inserts", "updates", "rejects"]])
        show_table(runs)
        st.caption("Latest 100 runs. Dates and times are UTC.")
        st.subheader("Recent delivery batches")
        show_table(query(conn, """SELECT batch_id,source_key,status,created_at,processed_at
            FROM ingestion_batches ORDER BY batch_id DESC LIMIT 100"""))
    with customers:
        search = st.text_input("Search customer ID, name or email", max_chars=255).strip()
        show_table(query(conn, """SELECT customer_id,name,email,phone,address,
            source_updated_at,start_date FROM customer_master
            WHERE active_flag=1 AND (%s='' OR LOCATE(%s,customer_id)>0
                OR LOCATE(LOWER(%s),LOWER(name))>0 OR LOCATE(LOWER(%s),LOWER(email))>0)
            ORDER BY customer_id LIMIT 500""", (search, search, search, search)))
        st.caption("Up to 500 matching current customers. Narrow the search for larger datasets.")
    with history:
        customer_id = st.text_input("Enter an exact customer ID", max_chars=64).strip()
        if customer_id:
            show_table(query(conn, """SELECT master_id,customer_id,name,email,phone,address,
                active_flag,start_date,end_date,source_updated_at,source_sequence,run_id
                FROM customer_master WHERE customer_id=%s
                ORDER BY start_date DESC,master_id DESC LIMIT 500""", (customer_id,)))
            st.caption("Latest 500 versions. Start/end dates describe ETL load time; source time is separate.")
        else:
            st.info("Enter a customer ID to inspect its SCD Type 2 versions.")
    with rejects:
        reasons = query(conn, """SELECT reason,COUNT(*) AS rejected_rows
            FROM customer_rejects GROUP BY reason ORDER BY rejected_rows DESC LIMIT 20""")
        if not reasons.empty:
            st.bar_chart(reasons.set_index("reason"))
        st.caption("Top 20 recorded reason combinations across all batches.")
        show_table(query(conn, """SELECT reject_id,batch_id,run_id,customer_id,name,email,
            reason,rejected_at FROM customer_rejects ORDER BY reject_id DESC LIMIT 500"""))
        st.caption("Latest 500 rejected rows.")


def main():
    st.title("Customer ETL dashboard")
    st.caption("Customer history, delivery status and pipeline quality • All timestamps UTC")
    st.sidebar.header("Dashboard")
    st.sidebar.button("Refresh data")
    st.sidebar.caption("Reads from your local MySQL database. Refresh after an ETL run.")
    conn = None
    try:
        conn = connect()
        conn.start_transaction(readonly=True, consistent_snapshot=True)
        render(conn)
    except ValueError:
        st.error("Database configuration is missing or invalid. Check config/db.env and MYSQL_PORT.")
    except mysql.connector.Error as exc:
        if exc.errno in (1146, 1054):
            st.error("The ETL database schema is missing or outdated. Initialize the database or run its migration.")
        else:
            st.error("Cannot read the MySQL database. Start MySQL and check the connection settings in config/db.env.")
        st.code("docker compose --env-file config/db.env up -d --wait mysql", language="bash")
    finally:
        if conn is not None:
            conn.rollback()
            conn.close()


if __name__ == "__main__":
    main()
