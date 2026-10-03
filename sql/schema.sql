-- Fresh-install schema. Existing databases must use scripts.migrate.
CREATE DATABASE IF NOT EXISTS customer_db CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
USE customer_db;

CREATE TABLE IF NOT EXISTS ingestion_batches (
    batch_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_key VARCHAR(255) NOT NULL UNIQUE,
    status ENUM('OPEN','READY','PROCESSED') NOT NULL DEFAULT 'OPEN',
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    processed_at DATETIME(6),
    run_id BIGINT,
    KEY idx_batch_status (status,batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS customer_staging (
    source_row_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    customer_id TEXT, name TEXT, email TEXT, phone TEXT, address TEXT,
    source_updated_at DATETIME(6),
    source_sequence BIGINT,
    KEY idx_staging_batch (batch_id,source_row_id),
    FOREIGN KEY (batch_id) REFERENCES ingestion_batches(batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS customer_master (
    master_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    name VARCHAR(255), email VARCHAR(255), phone VARCHAR(64), address VARCHAR(512),
    hash_val CHAR(64) NOT NULL,
    hash_version INT NOT NULL DEFAULT 2,
    active_flag TINYINT NOT NULL DEFAULT 1,
    active_customer_id VARCHAR(64) GENERATED ALWAYS AS
        (CASE WHEN active_flag=1 THEN customer_id ELSE NULL END) STORED,
    start_date DATETIME(6) NOT NULL, end_date DATETIME(6),
    updated_at DATETIME(6) NOT NULL, etl_loaded_at DATETIME(6) NOT NULL,
    source_updated_at DATETIME(6), source_sequence BIGINT,
    run_id BIGINT,
    UNIQUE KEY uq_one_active_customer (active_customer_id),
    KEY idx_customer_active (customer_id,active_flag),
    CHECK (active_flag IN (0,1)),
    CHECK ((active_flag=1 AND end_date IS NULL) OR (active_flag=0 AND end_date IS NOT NULL)),
    CHECK (end_date IS NULL OR end_date>=start_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS customer_rejects (
    reject_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_row_id BIGINT, batch_id BIGINT, run_id BIGINT,
    customer_id TEXT, name TEXT, email TEXT, phone TEXT, address TEXT,
    reason TEXT, rejected_at DATETIME(6),
    UNIQUE KEY uq_reject_source (source_row_id),
    KEY idx_reject_run (run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id BIGINT,
    started_at DATETIME(6), finished_at DATETIME(6),
    extracted BIGINT DEFAULT 0, standardized BIGINT DEFAULT 0,
    valid_rows BIGINT DEFAULT 0, inserts BIGINT DEFAULT 0,
    updates BIGINT DEFAULT 0, rejects BIGINT DEFAULT 0,
    status VARCHAR(20) DEFAULT 'RUNNING', message TEXT,
    KEY idx_run_batch (batch_id,status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- Disposable run-scoped work; partial Spark writes cannot affect the dimension.
CREATE TABLE IF NOT EXISTS customer_work (
    run_id BIGINT NOT NULL, source_row_id BIGINT NOT NULL, batch_id BIGINT NOT NULL,
    customer_id TEXT, name TEXT, email TEXT, phone TEXT, address TEXT,
    source_updated_at DATETIME(6), source_sequence BIGINT,
    hash_val CHAR(64), reason TEXT, action VARCHAR(20),
    PRIMARY KEY (run_id,source_row_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- Lock the parent row before inserting: sealing waits for concurrent writers.
DELIMITER $$
CREATE TRIGGER staging_insert_open BEFORE INSERT ON customer_staging FOR EACH ROW
BEGIN
    DECLARE batch_status VARCHAR(20);
    SELECT status INTO batch_status FROM ingestion_batches WHERE batch_id=NEW.batch_id FOR SHARE;
    IF batch_status IS NULL OR batch_status<>'OPEN' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Only OPEN batches accept staging rows';
    END IF;
END$$
CREATE TRIGGER staging_immutable_update BEFORE UPDATE ON customer_staging FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Staging rows are immutable; ingest a new batch';
END$$
CREATE TRIGGER staging_immutable_delete BEFORE DELETE ON customer_staging FOR EACH ROW
BEGIN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Staging rows are immutable; use an archival migration';
END$$
CREATE TRIGGER batches_forward_only BEFORE UPDATE ON ingestion_batches FOR EACH ROW
BEGIN
    IF NOT ((OLD.status='OPEN' AND NEW.status='READY') OR
            (OLD.status='READY' AND NEW.status='PROCESSED')) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Batches transition OPEN -> READY -> PROCESSED only';
    END IF;
    IF NEW.batch_id<>OLD.batch_id OR NEW.source_key<>OLD.source_key THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Batch identity is immutable';
    END IF;
END$$
DELIMITER ;
