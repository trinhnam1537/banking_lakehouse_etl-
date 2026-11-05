-- silver_fx.cnb_czk_rate — ty gia CNB, CZK cho 1 don vi ngoai te, moi ngay lam viec (16 file nam, ~4 nghin dong).
-- Sinh tu entry `cnb_czk_rate` (simple overwrite_table: CNB chup lai toan bo moi lan). Khong partition (bang nho).
-- Khoa: rate_date. Dong tien chua ton tai o nam do -> NULL.
CREATE TABLE IF NOT EXISTS glue_catalog.silver_fx.cnb_czk_rate (
    `rate_date` date COMMENT 'Ngay cong bo ty gia (ngay lam viec)',
    `usd_czk` decimal(18,6) COMMENT 'CZK cho 1 USD',
    `eur_czk` decimal(18,6) COMMENT 'CZK cho 1 EUR (tu 1999)',
    `gbp_czk` decimal(18,6) COMMENT 'CZK cho 1 GBP',
    `chf_czk` decimal(18,6) COMMENT 'CZK cho 1 CHF',
    `cad_czk` decimal(18,6) COMMENT 'CZK cho 1 CAD',
    `aud_czk` decimal(18,6) COMMENT 'CZK cho 1 AUD',
    `cny_czk` decimal(18,6) COMMENT 'CZK cho 1 CNY',
    `mxn_czk` decimal(18,6) COMMENT 'CZK cho 1 MXN',
    `brl_czk` decimal(18,6) COMMENT 'CZK cho 1 BRL',
    `ils_czk` decimal(18,6) COMMENT 'CZK cho 1 ILS',
    `xdr_czk` decimal(18,6) COMMENT 'CZK cho 1 SDR (IMF)',
    `jpy_czk` decimal(18,8) COMMENT 'CZK cho 1 JPY (CNB niem yet theo 100 JPY)',
    `inr_czk` decimal(18,8) COMMENT 'CZK cho 1 INR (niem yet theo 100 INR)',
    `rub_czk` decimal(18,8) COMMENT 'CZK cho 1 RUB (niem yet theo 100 RUB; ngung tu 03/2022)',
    `source_year_file` string COMMENT 'File nam goc cua CNB',
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
COMMENT 'Ty gia CNB - CZK cho 1 don vi ngoai te, moi ngay lam viec tu 1993'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
