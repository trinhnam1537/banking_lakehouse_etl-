-- silver_mbd.client_split — MBD-mini: khach -> fold 0..4. Sinh tu entry `mbd_client_split`
-- (overwrite_table). Khong partition. Khoa: client_id.
CREATE TABLE IF NOT EXISTS glue_catalog.silver_mbd.client_split (
    `client_id` string COMMENT 'Ma khach doanh nghiep (hash), khop target_month.client_id',
    `fold` int COMMENT 'Fold 0..4',
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
COMMENT 'MBD-mini - phan khach vao fold 0..4'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
