-- silver_aml.label — nhan rua tien tung giao dich, den tre 14 ngay (chi huan luyen / danh gia).
-- Sinh tu entry `aml_label` (simple append). Khong partition: 2 cot, ~32 trieu dong, file Parquet nho.
-- Khoa: txn_id -> silver_aml.transfer.
CREATE TABLE IF NOT EXISTS glue_catalog.silver_aml.label (
    `txn_id` string COMMENT 'Khoa noi voi silver_aml.transfer',
    `is_laundering` boolean COMMENT 'true = giao dich thuoc mot vu rua tien',
    `label_file` string COMMENT 'File nhan goc trong _inbox (ngay nhan duoc biet)',
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
COMMENT 'IBM AML - nhan rua tien tung giao dich (den tre 14 ngay)'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
