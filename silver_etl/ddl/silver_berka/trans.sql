-- Sinh tu entry `berka_trans` (writer: simple), SUA TAY mot cho:
--   PARTITIONED BY (months(ds_partition_date)) — partition transform cua Iceberg.
--   ds_partition_date = ngay nghiep vu (trans_date, 1993-1998). Theo ngay thi ~2.190 partition
--   x ~480 dong = qua nhieu file nho; theo thang 72 partition x ~14,7 nghin dong.
--   Truy van loc `ds_partition_date = DATE'...'` van duoc Iceberg cat tia theo thang.
--   Giong silver_corebank/trans.sql.
-- runner van tu them cot moi cua entry (ALTER TABLE ADD COLUMNS).
CREATE TABLE IF NOT EXISTS glue_catalog.silver_berka.trans (
    `trans_id` bigint,
    `account_id` int,
    `trans_date` date COMMENT 'Ngay nghiep vu',
    `trans_type_code` string COMMENT 'PRIJEM | VYDAJ | VYBER (loi goc)',
    `operation_code` string,
    `amount` decimal(18,2) COMMENT 'Luon duong; dau xem direction',
    `balance_after` decimal(18,2) COMMENT 'So du sau giao dich, co the am',
    `k_symbol` string,
    `counterparty_bank` string,
    `counterparty_account` bigint COMMENT '0 o nguon = khong co doi ung (rut tien mat / rut the) -> NULL',
    `direction` string,
    `signed_amount` decimal(18,2),
    `operation_type` string,
    `purpose` string,
    `is_overdraft` boolean,
    `is_berka_type_error` boolean COMMENT 'Loi goc Berka: rut tien ghi o cot type',
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
PARTITIONED BY (months(ds_partition_date))
COMMENT 'So giao dich tai khoan (Berka goc) - su kien, append theo manifest'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
