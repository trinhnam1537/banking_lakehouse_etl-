-- silver_opensanctions.entity — thuc the FtM day du (~305 nghin, ca thuc the lien quan), lich su SCD2.
-- Sinh tu entry `opensanctions_entity` (scd2, snapshot). properties: map<string, array<string>>.
-- Hien hanh: WHERE ds_is_current AND ds_record_status <> 'D'. Khoa: entity_id.
CREATE TABLE IF NOT EXISTS glue_catalog.silver_opensanctions.entity (
    `entity_id` string COMMENT 'Ma OpenSanctions (NK-... hoac ma nguon)',
    `entity_schema` string COMMENT 'Loai FtM: Person, Company, Ownership, Directorship, Address, ...',
    `caption` string COMMENT 'Nhan hien thi',
    `is_target` boolean COMMENT 'true = doi tuong bi cam van; false = thuc the lien quan',
    `referents` array<string> COMMENT 'Ma goc o cac nguon da hop nhat',
    `source_datasets` array<string> COMMENT 'Nguon chua thuc the',
    `origin` array<string> COMMENT 'Cach OpenSanctions suy ra (metadata, inferred, patch)',
    `properties` map<string,array<string>> COMMENT 'Thuoc tinh FtM: name, birthDate, country, ownerId/assetId (Ownership)...',
    `first_seen` timestamp COMMENT 'Lan dau thay',
    `last_seen` timestamp COMMENT 'Lan gan nhat thay (doi moi lan crawl)',
    `last_change` timestamp COMMENT 'Lan gan nhat noi dung doi',
    `ds_key` string COMMENT 'Khoa nghiep vu bam SHA-256 (technical.keys)',
    `ds_hash` string COMMENT 'Bam cac cot nghiep vu — phat hien thay doi',
    `ds_record_status` string COMMENT 'I=moi, U=doi, D=xoa, N=khong doi (scd4a)',
    `ds_partition_date` date COMMENT 'Ngay du lieu (technical.partition_date)',
    `ds_cdc_index` bigint COMMENT 'Thu tu CDC: micro-giay hoac so tang dan',
    `ds_etl_timestamp` timestamp COMMENT 'Thoi diem ghi',
    `ds_etl_job_id` string COMMENT 'run_id cua lan chay ghi dong nay',
    `ds_source` string COMMENT 'Manifest (hoac prefix) ma dong nay den tu',
    `ds_valid_from` date COMMENT 'Phien ban co hieu luc tu ngay (gom)',
    `ds_valid_to` date COMMENT 'Het hieu luc tu ngay (khong gom); 9999-12-31 = hien hanh',
    `ds_is_current` boolean COMMENT 'Phien ban hien hanh'
)
USING iceberg
COMMENT 'OpenSanctions - thuc the FtM day du (thuoc tinh dang map), lich su SCD2'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
