# etl_customer_master.py

import os
from datetime import datetime
from dotenv import load_dotenv
import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, sha2, concat_ws, lit, current_timestamp, coalesce
from pyspark.sql.types import TimestampType


# Load DB Config

load_dotenv("config/db.env")

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
MYSQL_DB = os.getenv("MYSQL_DB")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_JAR = "/Users/aagyashrestha/Downloads/mysql-connector-j-8.0.33/mysql-connector-j-8.0.33.jar"


# Spark Session

spark = SparkSession.builder \
    .appName("Customer_ETL") \
    .config("spark.jars", MYSQL_JAR) \
    .getOrCreate()


# ETL Logging Function

def log_etl(run_id, finished_at=None, extracted=0, standardized=0, valid_rows=0,
            inserts=0, updates=0, rejects=0, status='RUNNING', message=''):
    conn = mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
        password=MYSQL_PASSWORD, database=MYSQL_DB
    )
    cursor = conn.cursor()
    update_query = """
    UPDATE etl_run_log SET finished_at=%s, extracted=%s, standardized=%s,
    valid_rows=%s, inserts=%s, updates=%s, rejects=%s, status=%s, message=%s
    WHERE run_id=%s
    """
    cursor.execute(update_query, (finished_at, extracted, standardized, valid_rows,
                                  inserts, updates, rejects, status, message, run_id))
    conn.commit()
    cursor.close()
    conn.close()


# Start ETL Run Log

conn = mysql.connector.connect(
    host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
    password=MYSQL_PASSWORD, database=MYSQL_DB
)
cursor = conn.cursor()
cursor.execute("INSERT INTO etl_run_log (status, started_at) VALUES ('RUNNING', NOW())")
run_id = cursor.lastrowid
conn.commit()
cursor.close()
conn.close()

try:

    # Extract staging

    staging_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "customer_staging") \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .load()
    extracted_count = staging_df.count()


    # Standardize

    std_df = staging_df.withColumn("email", lower(trim(col("email")))) \
                       .withColumn("name", lower(trim(col("name")))) \
                       .withColumn("phone", trim(col("phone"))) \
                       .withColumn("address", trim(col("address")))
    standardized_count = std_df.count()


    # Validate 

    valid_df = std_df.filter(col("customer_id").isNotNull() &
                             col("name").isNotNull() &
                             col("phone").isNotNull())
    valid_count = valid_df.count()
    reject_df = std_df.subtract(valid_df)
    reject_count = reject_df.count()

    if reject_count > 0:
        reject_df.withColumn("reason", lit("Missing required fields: customer_id, name, phone")) \
                 .withColumn("rejected_at", current_timestamp()) \
                 .write.format("jdbc") \
                 .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}") \
                 .option("driver", "com.mysql.cj.jdbc.Driver") \
                 .option("dbtable", "customer_rejects") \
                 .option("user", MYSQL_USER) \
                 .option("password", MYSQL_PASSWORD) \
                 .mode("append") \
                 .save()

    
    # Hash calculation
    
    valid_df = valid_df.withColumn("hash_val", sha2(concat_ws("|",
                                                col("customer_id"),
                                                col("name"),
                                                coalesce(col("email"), lit("")),
                                                col("phone"),
                                                coalesce(col("address"), lit(""))
                                                ), 256))

    
    # Load active master
    
    master_df = spark.read.format("jdbc") \
        .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "(SELECT customer_id, hash_val, start_date FROM customer_master WHERE active_flag=1) AS tmp") \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .load() \
        .withColumnRenamed("hash_val", "hash_val_master")

    
    # Join staging with master
    
    joined_df = valid_df.alias("src").join(master_df.alias("tgt"), "customer_id", how="left")

    
    # Inserts: New customers
    
    inserts_df = joined_df.filter(col("hash_val_master").isNull()) \
        .withColumn("active_flag", lit(1)) \
        .withColumn("start_date", current_timestamp()) \
        .withColumn("end_date", lit(None).cast(TimestampType())) \
        .withColumn("updated_at", current_timestamp()) \
        .withColumn("etl_loaded_at", current_timestamp()) \
        .drop("hash_val_master")

    inserts_count = inserts_df.count()

    if inserts_count > 0:
        inserts_df.write.format("jdbc") \
            .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "customer_master") \
            .option("user", MYSQL_USER) \
            .option("password", MYSQL_PASSWORD) \
            .mode("append") \
            .save()

    
    # Updates: Changed customers
    
    updates_df = joined_df.filter(
        (col("hash_val_master").isNotNull()) & (col("hash_val") != col("hash_val_master"))
    )

    updates_count = updates_df.count()

    if updates_count > 0:
        # Deactivate old records
        updates_to_deactivate = updates_df.select(
            col("customer_id"),
            col("start_date"),
            lit(0).alias("active_flag"),
            current_timestamp().alias("end_date"),
            current_timestamp().alias("updated_at"),
            current_timestamp().alias("etl_loaded_at"),
            col("hash_val_master").alias("hash_val")
        )

        updates_to_deactivate.write.format("jdbc") \
            .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "customer_master") \
            .option("user", MYSQL_USER) \
            .option("password", MYSQL_PASSWORD) \
            .mode("append") \
            .save()

        # Insert new version of updated records
        updates_new_records = updates_df.withColumn("active_flag", lit(1)) \
                                       .withColumn("start_date", current_timestamp()) \
                                       .withColumn("end_date", lit(None).cast(TimestampType())) \
                                       .withColumn("updated_at", current_timestamp()) \
                                       .withColumn("etl_loaded_at", current_timestamp()) \
                                       .drop("hash_val_master")

        updates_new_records.write.format("jdbc") \
            .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "customer_master") \
            .option("user", MYSQL_USER) \
            .option("password", MYSQL_PASSWORD) \
            .mode("append") \
            .save()

    
    # Log ETL success
    
    log_etl(run_id, finished_at=datetime.now(), extracted=extracted_count,
            standardized=standardized_count, valid_rows=valid_count,
            inserts=inserts_count, updates=updates_count, rejects=reject_count,
            status='SUCCESS', message='ETL completed successfully.')

    print(f"""
ETL Run Completed Successfully!
Run ID: {run_id}
Records Extracted: {extracted_count}
Records Standardized: {standardized_count}
Valid Records: {valid_count}
Inserted Records: {inserts_count}
Updated Records: {updates_count}
Rejected Records: {reject_count}
""")

except Exception as e:
    log_etl(run_id, finished_at=datetime.now(), status='FAILED', message=str(e))
    raise e