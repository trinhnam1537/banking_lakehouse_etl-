-- Sinh tu entry `gleif_legal_entity` (writer: scd2). Sua tay duoc;
-- runner van tu them cot moi cua entry (ALTER TABLE ADD COLUMNS).
CREATE TABLE IF NOT EXISTS glue_catalog.track_a_silver.gleif_legal_entity (
    `lei` string COMMENT 'Legal Entity Identifier',
    `legal_name` string,
    `legal_name_language` string,
    `registered_as` string COMMENT 'Ma so doanh nghiep tai co quan dang ky',
    `registration_authority_id` string,
    `jurisdiction` string,
    `legal_form_code` string COMMENT 'Ma ELF (ISO 20275)',
    `entity_category` string,
    `entity_status` string,
    `legal_country` string,
    `legal_region` string,
    `legal_city` string,
    `hq_country` string,
    `registration_status` string,
    `initial_registration_ts` timestamp,
    `last_update_ts` timestamp,
    `next_renewal_ts` timestamp,
    `direct_parent_lei` string COMMENT 'Nguon canh fin:isDirectlyOwnedBy',
    `ultimate_parent_lei` string,
    `golden_copy_ts` timestamp,
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
COMMENT 'Phap nhan VN co LEI, kem LEI cong ty me truc tiep va cuoi cung'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
