# IncrementalCustomerETL

**Incremental Customer Master Data ETL Pipeline using PySpark and MySQL**  

This project implements a robust, incremental ETL pipeline for cleaning, validating, and maintaining customer master data. Designed for production-scale data processing, it efficiently handles inserts, updates, and rejected records while maintaining historical changes.  

---

## Project Overview

Customer master data often changes frequently, and keeping it clean, consistent, and up-to-date is crucial for analytics and operations. This ETL pipeline addresses common challenges:  

- **Data Standardization:** Cleans and formats names, emails, phone numbers, and addresses.  
- **Validation:** Filters out incomplete or invalid records while logging rejections.  
- **Incremental Loading:** Detects new or updated records using SHA-256 hash comparison.  
- **History Preservation:** Maintains historical changes with `active_flag`, `start_date`, and `end_date`.  
- **Scalability:** Built using PySpark for efficient processing of large datasets.  

---

## Key Features

- Extracts customer staging data from MySQL.  
- Standardizes and cleans data (`trim`, `lowercase`, `null handling`).  
- Validates essential fields (`customer_id`, `name`, `phone`).  
- Logs rejected records with reasons and timestamps in `customer_rejects`.  
- Calculates SHA-256 hash for incremental change detection.  
- Inserts new records and updates changed records in `customer_master`.  
- Maintains history with `active_flag`, `start_date`, and `end_date`.  
- Logs ETL run statistics in `etl_run_log` for auditing and monitoring.  

---

## Technologies Used

- **Apache Spark / PySpark**: Distributed processing and transformations.  
- **MySQL**: Relational database for staging, master, and reject tables.  
- **Python 3**: ETL orchestration and logging.  
- **dotenv**: Environment configuration.  
- **SHA-256 Hashing**: Change detection and data integrity.  

---
