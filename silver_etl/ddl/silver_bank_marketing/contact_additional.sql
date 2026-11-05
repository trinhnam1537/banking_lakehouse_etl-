-- silver_bank_marketing.contact_additional — UCI Bank Marketing bank-additional-full (41.188 dong)
-- kem 5 chi so kinh te vi mo. Sinh tu entry `bank_marketing_contact_additional` (overwrite_table). Khong partition.
-- Khoa: (source_file, source_row). Nhan UC2: subscribed.
CREATE TABLE IF NOT EXISTS glue_catalog.silver_bank_marketing.contact_additional (
    `source_file` string COMMENT 'File goc trong archive UCI',
    `source_row` bigint COMMENT 'So dong trong file goc',
    `age` int COMMENT 'Tuoi khach',
    `job` string COMMENT 'Nghe nghiep',
    `marital` string COMMENT 'married | single | divorced | unknown',
    `education` string COMMENT 'basic.4y | basic.6y | basic.9y | high.school | professional.course | university.degree | illiterate | unknown',
    `credit_default` string COMMENT 'Co no xau: yes | no | unknown',
    `housing_loan` string COMMENT 'Co vay mua nha',
    `personal_loan` string COMMENT 'Co vay tieu dung',
    `contact_channel` string COMMENT 'cellular | telephone',
    `last_contact_month` string COMMENT 'Thang lan goi cuoi',
    `last_contact_dow` string COMMENT 'Thu trong tuan: mon..fri',
    `duration_sec` int COMMENT 'Do dai cuoc goi (giay), ro ri nhan',
    `campaign_contacts` int COMMENT 'So lan goi trong dot nay',
    `days_since_prev_contact` int COMMENT 'So ngay tu lan goi truoc; 999 = chua tung goi',
    `prev_contacts` int COMMENT 'So lan goi truoc dot nay',
    `prev_outcome` string COMMENT 'success | failure | nonexistent',
    `emp_var_rate` double COMMENT 'Ty le bien dong viec lam (quy)',
    `cons_price_idx` double COMMENT 'Chi so gia tieu dung (thang)',
    `cons_conf_idx` double COMMENT 'Chi so niem tin tieu dung (thang)',
    `euribor_3m` double COMMENT 'Lai suat Euribor 3 thang (ngay)',
    `nr_employed` double COMMENT 'So nguoi co viec (nghin, quy)',
    `subscribed` boolean COMMENT 'NHAN UC2: dang ky tien gui co ky han',
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
COMMENT 'Bank Marketing (bank-additional-full) - lan goi + chi so kinh te vi mo'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
