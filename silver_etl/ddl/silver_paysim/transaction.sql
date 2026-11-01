-- silver_paysim.transaction — giao dich vi dien tu PaySim (6,36 trieu dong / 31 ngay).
-- Sinh tu entry `paysim_transaction` (simple append), SUA TAY:
--   PARTITIONED BY (ds_partition_date): = ngay nghiep vu (txn_date), ~205 nghin dong/ngay.
-- Khoa: txn_ref -> silver_paysim.fraud_label.
CREATE TABLE IF NOT EXISTS glue_catalog.silver_paysim.transaction (
    `txn_ref` string COMMENT 'Ma doi soat do doi tac cap (PS + so dong goc)',
    `txn_ts` timestamp COMMENT 'Thoi diem giao dich tren truc chung, UTC',
    `step` int COMMENT 'Gio mo phong goc 1..744',
    `txn_type` string COMMENT 'CASH_IN | CASH_OUT | DEBIT | PAYMENT | TRANSFER',
    `amount` decimal(18,2) COMMENT 'So tien giao dich',
    `orig_account` string COMMENT 'Vi gui (C... = khach)',
    `orig_balance_before` decimal(18,2) COMMENT 'So du vi gui truoc giao dich',
    `orig_balance_after` decimal(18,2) COMMENT 'So du vi gui sau giao dich',
    `dest_account` string COMMENT 'Vi nhan: C... = khach, M... = merchant',
    `dest_balance_before` decimal(18,2) COMMENT 'Merchant luon 0 (gioi han cua PaySim)',
    `dest_balance_after` decimal(18,2) COMMENT 'So du vi nhan sau giao dich',
    `txn_date` date COMMENT 'Ngay nghiep vu (UTC)',
    `dest_type` string COMMENT 'MERCHANT | CUSTOMER (theo tien to dest_account)',
    `balance_mismatch_orig` boolean COMMENT 'So du ben gui khong khop (orig_before - amount <> orig_after) — loi da biet cua PaySim, dac trung huu ich',
    `ds_key` string COMMENT 'Khoa nghiep vu bam SHA-256 (technical.keys)',
    `ds_hash` string COMMENT 'Bam cac cot nghiep vu — phat hien thay doi',
    `ds_record_status` string COMMENT 'I=moi, U=doi, D=xoa, N=khong doi (scd4a)',
    `ds_partition_date` date COMMENT 'Ngay du lieu (technical.partition_date)',
    `ds_cdc_index` bigint COMMENT 'Thu tu CDC: micro-giay hoac so tang dan',
    `ds_etl_timestamp` timestamp COMMENT 'Thoi diem ghi',
    `ds_etl_job_id` string COMMENT 'run_id cua lan chay ghi dong nay',
    `ds_source` string COMMENT 'Manifest (hoac prefix) ma dong nay den tu'
)
USING iceberg
PARTITIONED BY (ds_partition_date)
COMMENT 'PaySim - giao dich vi dien tu tu file doi soat cuoi ngay (khong nhan)'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
