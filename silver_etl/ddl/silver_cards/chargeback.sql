-- silver_cards.chargeback — nhan gian lan the tung giao dich, den tre 20 ngay (chi danh gia / huan luyen).
-- Sinh tu entry `cards_chargeback` (simple append). Khong partition: 2 cot, vai trieu dong.
-- Khoa: trans_num -> silver_cards.card_authorization.
CREATE TABLE IF NOT EXISTS glue_catalog.silver_cards.chargeback (
    `trans_num` string COMMENT 'Khoa noi voi silver_cards.card_authorization',
    `is_fraud` boolean COMMENT 'true = giao dich gian lan (chargeback)',
    `label_file` string COMMENT 'File nhan goc trong _inbox: labels_<ngay giao dich>.csv.gz',
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
COMMENT 'The - nhan gian lan tung giao dich qua chargeback (den tre 20 ngay)'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
