-- silver_opensanctions.target — doi tuong bi cam van (targets.simple.csv, ~72 nghin), lich su SCD2.
-- Sinh tu entry `opensanctions_target` (scd2, snapshot: vang mat -> D). Khong partition.
-- Hien hanh: WHERE ds_is_current AND ds_record_status <> 'D'. Khoa: entity_id. Cot PII: phones, emails.
CREATE TABLE IF NOT EXISTS glue_catalog.silver_opensanctions.target (
    `entity_id` string COMMENT 'Ma OpenSanctions (NK-...), on dinh giua cac lan chup',
    `entity_schema` string COMMENT 'Loai FtM: Person | Organization | Company | Vessel | Airplane | LegalEntity',
    `name` string COMMENT 'Ten chinh',
    `birth_date` string COMMENT 'Ngay sinh/thanh lap (chuoi: co the chi co nam, nhieu gia tri noi \';\')',
    `sanctions` string COMMENT 'Mo ta bien phap cam van (van ban tu do)',
    `first_seen` timestamp COMMENT 'Lan dau OpenSanctions thay doi tuong',
    `last_seen` timestamp COMMENT 'Lan gan nhat thay (doi moi lan crawl)',
    `last_change` timestamp COMMENT 'Lan gan nhat noi dung doi',
    `aliases` array<string> COMMENT 'Ten khac',
    `countries` array<string> COMMENT 'Ma quoc gia ISO-2 (chu thuong)',
    `addresses` array<string> COMMENT 'Dia chi',
    `identifiers` array<string> COMMENT 'Ma dinh danh: IMO, so ho chieu, ma so thue...',
    `phones` array<string> COMMENT 'PII: so dien thoai',
    `emails` array<string> COMMENT 'PII: email',
    `program_ids` array<string> COMMENT 'Chuong trinh cam van: US-GLOMAG, EU-RUS...',
    `source_datasets` array<string> COMMENT 'Danh sach nguon (OFAC SDN, EU...)',
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
COMMENT 'OpenSanctions - doi tuong bi cam van (nguoi, to chuc, tau...), lich su SCD2'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
