-- silver_paysim.fraud_label — nhan gian lan PaySim, den tre 30 ngay (chi huan luyen / danh gia).
-- Sinh tu entry `paysim_fraud_label` (simple append). Khong partition.
-- Khoa: txn_ref -> silver_paysim.transaction.
CREATE TABLE IF NOT EXISTS glue_catalog.silver_paysim.fraud_label (
    `txn_ref` string COMMENT 'Khoa noi voi silver_paysim.transaction',
    `is_fraud` boolean COMMENT 'Gian lan that (isFraud)',
    `is_flagged_fraud` boolean COMMENT 'He thong cu da gan co (isFlaggedFraud: TRANSFER > 200.000)',
    `label_file` string COMMENT 'File nhan goc trong _inbox',
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
COMMENT 'PaySim - nhan gian lan tung giao dich (den tre 30 ngay)'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
