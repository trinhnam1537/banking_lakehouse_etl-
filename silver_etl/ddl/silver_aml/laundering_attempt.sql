-- silver_aml.laundering_attempt — giao dich thuoc tung vu rua tien + kieu hinh (ground truth do thi).
-- Sinh tu entry `aml_laundering_attempt` (simple append). Nho (vai chuc nghin dong), khong partition.
-- Khoa: (attempt_id, txn_id). source_ts la thoi gian GOC (09/2022), chua doi truc.
CREATE TABLE IF NOT EXISTS glue_catalog.silver_aml.laundering_attempt (
    `attempt_id` string COMMENT 'Ma vu, vd HI-Medium-00042',
    `typology` string COMMENT 'FAN-OUT | FAN-IN | CYCLE | SCATTER-GATHER | GATHER-SCATTER | BIPARTITE | STACK | RANDOM',
    `typology_detail` string COMMENT 'Chi tiet kieu hinh, vd \'Max 16-degree Fan-Out\'',
    `txn_id` string COMMENT 'Khoa noi voi silver_aml.transfer / label',
    `source_ts` timestamp COMMENT 'Thoi gian goc IBM, chua doi truc',
    `from_bank` string COMMENT 'Ma ngan hang gui',
    `from_account` string COMMENT 'Tai khoan gui',
    `to_bank` string COMMENT 'Ma ngan hang nhan',
    `to_account` string COMMENT 'Tai khoan nhan',
    `amount_received` decimal(28,8) COMMENT 'So tien nhan, theo receiving_currency',
    `receiving_currency` string COMMENT 'Tien nhan',
    `amount_paid` decimal(28,8) COMMENT 'So tien tra, theo payment_currency',
    `payment_currency` string COMMENT 'Tien tra',
    `payment_format` string COMMENT 'Hinh thuc thanh toan',
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
COMMENT 'IBM AML - giao dich thuoc tung vu rua tien, kem kieu hinh (FAN-OUT, CYCLE...)'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
