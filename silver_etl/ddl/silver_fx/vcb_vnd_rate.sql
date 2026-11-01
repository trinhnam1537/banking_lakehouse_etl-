-- silver_fx.vcb_vnd_rate — ty gia Vietcombank, VND cho 1 don vi ngoai te (~20 dong tien/ngay tu 06/2020).
-- Sinh tu entry `vcb_vnd_rate` (simple append theo manifest). Khong partition (bang nho).
-- Khoa: (rate_date, currency_code).
CREATE TABLE IF NOT EXISTS glue_catalog.silver_fx.vcb_vnd_rate (
    `published_at` timestamp COMMENT 'Thoi diem VCB cap nhat bang ty gia (UTC)',
    `currency_code` string COMMENT 'Ma ISO 4217: USD, EUR, JPY, ...',
    `currency_name` string COMMENT 'Ten dong tien theo VCB',
    `buy_cash` decimal(18,4) COMMENT 'VND/1 don vi: mua tien mat (NULL neu VCB khong mua)',
    `buy_transfer` decimal(18,4) COMMENT 'VND/1 don vi: mua chuyen khoan',
    `sell` decimal(18,4) COMMENT 'VND/1 don vi: ban ra',
    `rate_date` date COMMENT 'Ngay ap dung ty gia',
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
COMMENT 'Ty gia Vietcombank - VND cho 1 don vi ngoai te (mua tien mat, mua chuyen khoan, ban)'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
