-- silver_bank_marketing.contact_full — UCI Bank Marketing bank-full (45.211 lan goi, 2008-2010).
-- Sinh tu entry `bank_marketing_contact_full` (simple overwrite_table: bo tinh, CDC5). Khong partition.
-- Khoa: (source_file, source_row) — nguon khong co ma khach. Nhan UC2: subscribed.
CREATE TABLE IF NOT EXISTS glue_catalog.silver_bank_marketing.contact_full (
    `source_file` string COMMENT 'File goc trong archive UCI',
    `source_row` bigint COMMENT 'So dong trong file goc (khoa cung source_file)',
    `age` int COMMENT 'Tuoi khach',
    `job` string COMMENT 'Nghe nghiep: admin., blue-collar, management, ... , unknown',
    `marital` string COMMENT 'married | single | divorced (divorced gom ca goa)',
    `education` string COMMENT 'primary | secondary | tertiary | unknown',
    `credit_default` string COMMENT 'Co no xau: yes | no | unknown',
    `balance_eur` decimal(18,2) COMMENT 'So du binh quan nam (EUR), co the am',
    `housing_loan` string COMMENT 'Co vay mua nha: yes | no | unknown',
    `personal_loan` string COMMENT 'Co vay tieu dung: yes | no | unknown',
    `contact_channel` string COMMENT 'cellular | telephone | unknown',
    `last_contact_day` int COMMENT 'Ngay trong thang cua lan goi cuoi',
    `last_contact_month` string COMMENT 'Thang lan goi cuoi: jan..dec',
    `duration_sec` int COMMENT 'Do dai cuoc goi (giay). CHI biet sau cuoc goi -> khong dung lam dac trung du doan',
    `campaign_contacts` int COMMENT 'So lan goi trong dot nay (ke ca lan nay)',
    `days_since_prev_contact` int COMMENT 'So ngay tu lan goi dot truoc; -1 = chua tung goi',
    `prev_contacts` int COMMENT 'So lan goi truoc dot nay',
    `prev_outcome` string COMMENT 'Ket qua dot truoc: success | failure | other | unknown',
    `subscribed` boolean COMMENT 'NHAN UC2: khach dang ky tien gui co ky han (fin:TermDeposit)',
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
COMMENT 'Bank Marketing (bank-full) - lan goi telemarketing va ket qua dang ky tien gui co ky han'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
