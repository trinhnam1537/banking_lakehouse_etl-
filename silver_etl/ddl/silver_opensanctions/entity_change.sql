-- silver_opensanctions.entity_change — su kien ADD/MOD/DEL theo phien ban delta (6 gio/lan).
-- Sinh tu entry `opensanctions_entity_change` (simple append), SUA TAY: PARTITIONED BY (ds_partition_date)
--   = ngay phat hanh phien ban. Khoa: (delta_version, entity_id).
CREATE TABLE IF NOT EXISTS glue_catalog.silver_opensanctions.entity_change (
    `delta_version` string COMMENT 'Phien ban delta, vd 20261008075007-crk (thu tu thoi gian)',
    `op` string COMMENT 'ADD | MOD | DEL',
    `entity_id` string COMMENT 'Ma thuc the',
    `entity_schema` string COMMENT 'Loai FtM',
    `caption` string COMMENT 'Nhan hien thi',
    `is_target` boolean COMMENT 'Doi tuong bi cam van',
    `source_datasets` array<string> COMMENT 'Nguon chua thuc the',
    `properties` map<string,array<string>> COMMENT 'Thuoc tinh FtM sau thay doi (rong voi DEL)',
    `last_change` timestamp COMMENT 'Thoi diem noi dung doi',
    `version_ts` timestamp COMMENT 'Thoi diem phat hanh phien ban (tu delta_version)',
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
COMMENT 'OpenSanctions - su kien thay doi thuc the theo phien ban delta'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
