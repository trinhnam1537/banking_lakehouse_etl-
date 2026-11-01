-- silver_mbd.target_month — MBD-mini: khach doanh nghiep x thang, nhan mua 4 san pham an danh.
-- Sinh tu entry `mbd_target_month` (overwrite_table), SUA TAY: PARTITIONED BY (ds_partition_date)
--   = report_month (~12 thang x ~100 nghin dong). Khoa: (client_id, report_month).
CREATE TABLE IF NOT EXISTS glue_catalog.silver_mbd.target_month (
    `client_id` string COMMENT 'Ma khach doanh nghiep (hash SHA-256 cua Sber)',
    `report_month` date COMMENT 'Ngay cuoi thang bao cao',
    `target_1` boolean COMMENT 'Mua san pham an danh 1 trong thang sau',
    `target_2` boolean COMMENT 'Mua san pham an danh 2 trong thang sau',
    `target_3` boolean COMMENT 'Mua san pham an danh 3 trong thang sau',
    `target_4` boolean COMMENT 'Mua san pham an danh 4 trong thang sau',
    `trans_count` int COMMENT 'So giao dich cua khach trong thang',
    `diff_trans_date` int COMMENT 'Khoang ngay giua giao dich dau va cuoi trong thang',
    `fold` int COMMENT 'Fold 0..4 cua bo du lieu (chia train/test theo khach)',
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
COMMENT 'MBD-mini - nhan mua 4 san pham an danh theo khach x thang'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
