-- silver_aml.account — tai khoan IBM AML -> phap nhan so huu (~2 trieu dong, tinh).
-- Sinh tu entry `aml_account` (simple append). Khong partition (bang tham chieu).
-- Khoa: (bank_id, account_number). entity_id gom tai khoan ve chu the (nut Party tren do thi).
CREATE TABLE IF NOT EXISTS glue_catalog.silver_aml.account (
    `bank_id` string COMMENT 'Ma ngan hang, khop transfer.from_bank / to_bank',
    `bank_name` string COMMENT 'Ten ngan hang tong hop',
    `account_number` string COMMENT 'Ma tai khoan, khop transfer.from_account / to_account',
    `entity_id` string COMMENT 'Ma phap nhan so huu; mot phap nhan co the co nhieu tai khoan',
    `entity_name` string COMMENT 'Nhan tong hop, vd \'Corporation #33520\' (khong phai ten that)',
    `entity_type` string COMMENT 'Loai phap nhan tach tu entity_name: Corporation | Sole Proprietorship | Partnership | ...',
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
COMMENT 'IBM AML - danh muc tai khoan va phap nhan so huu'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
