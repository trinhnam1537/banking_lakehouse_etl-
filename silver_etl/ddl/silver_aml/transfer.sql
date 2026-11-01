-- silver_aml.transfer — giao dich chuyen tien lien ngan hang IBM AML (HI-Medium ~32 trieu dong).
-- Sinh tu entry `aml_transfer` (writer: simple append), SUA TAY:
--   PARTITIONED BY (ds_partition_date): ds_partition_date = ngay nghiep vu (event_date).
--   ~16 ngay x ~2 trieu dong: partition theo ngay vua co, loc theo ngay cat tia duoc.
-- Khoa: txn_id. Noi: label.txn_id, laundering_attempt.txn_id,
--   account (bank_id, account_number) = (from_bank, from_account) | (to_bank, to_account).
-- runner van tu them cot moi cua entry (ALTER TABLE ADD COLUMNS).
CREATE TABLE IF NOT EXISTS glue_catalog.silver_aml.transfer (
    `txn_id` string COMMENT 'Ma giao dich tat dinh (sha1 noi dung + lan xuat hien), noi voi label/pattern',
    `event_ts` timestamp COMMENT 'Thoi diem giao dich da doi ve truc chung (SIM_EPOCH), UTC, do phan giai phut',
    `source_ts` string COMMENT 'Thoi diem goc trong file IBM (yyyy/MM/dd HH:mm, 09/2022)',
    `from_bank` string COMMENT 'Ma ngan hang gui (chuoi, giu so 0 dau)',
    `from_account` string COMMENT 'Tai khoan gui, chi duy nhat khi di cung from_bank',
    `to_bank` string COMMENT 'Ma ngan hang nhan',
    `to_account` string COMMENT 'Tai khoan nhan',
    `amount_received` decimal(28,8) COMMENT 'So tien ben nhan nhan duoc, theo receiving_currency',
    `receiving_currency` string COMMENT 'Tien nhan (15 loai, gom Bitcoin)',
    `amount_paid` decimal(28,8) COMMENT 'So tien ben gui tra, theo payment_currency',
    `payment_currency` string COMMENT 'Tien tra',
    `payment_format` string COMMENT 'ACH | Cheque | Credit Card | Wire | Cash | Bitcoin | Reinvestment',
    `event_date` date COMMENT 'Ngay nghiep vu (UTC)',
    `is_self_transfer` boolean COMMENT 'Tu chuyen cho chinh minh (thuong Reinvestment) -> self-loop, loai khoi canh do thi',
    `is_cross_currency` boolean COMMENT 'Tien nhan khac tien tra (co quy doi)',
    `fx_rate_paid_per_received` decimal(28,12) COMMENT 'Ty gia ngam: don vi tien tra cho 1 don vi tien nhan',
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
COMMENT 'IBM AML - giao dich chuyen tien lien ngan hang (khong nhan), append theo manifest'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
