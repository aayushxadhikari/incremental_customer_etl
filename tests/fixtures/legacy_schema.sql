CREATE TABLE customer_staging (
    customer_id VARCHAR(64), name VARCHAR(255), email VARCHAR(255),
    phone VARCHAR(64), address VARCHAR(512)
);
CREATE TABLE customer_master (
    master_id BIGINT AUTO_INCREMENT PRIMARY KEY, customer_id VARCHAR(64) NOT NULL,
    name VARCHAR(255), email VARCHAR(255), phone VARCHAR(64), address VARCHAR(512),
    hash_val CHAR(64), active_flag TINYINT NOT NULL DEFAULT 1,
    start_date TIMESTAMP NULL, end_date TIMESTAMP NULL, updated_at TIMESTAMP NULL,
    etl_loaded_at TIMESTAMP NULL, KEY idx_customer_active (customer_id,active_flag)
);
CREATE TABLE customer_rejects (
    customer_id VARCHAR(64), name VARCHAR(255), email VARCHAR(255), phone VARCHAR(64),
    address VARCHAR(512), reason VARCHAR(255), rejected_at TIMESTAMP NULL
);
CREATE TABLE etl_run_log (
    run_id BIGINT AUTO_INCREMENT PRIMARY KEY, started_at TIMESTAMP NULL,
    finished_at TIMESTAMP NULL, extracted INT DEFAULT 0, standardized INT DEFAULT 0,
    valid_rows INT DEFAULT 0, inserts INT DEFAULT 0, updates INT DEFAULT 0,
    rejects INT DEFAULT 0, status VARCHAR(20) DEFAULT 'RUNNING', message TEXT
);
