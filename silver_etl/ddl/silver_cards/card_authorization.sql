-- silver_cards.card_authorization — giao dich the tu Kafka card.authorizations (~170 nghin/ngay).
-- Sinh tu entry `cards_authorization` (simple append theo micro-batch), SUA TAY:
--   PARTITIONED BY (ds_partition_date) = ngay giao dich (txn_date).
-- Khoa: trans_num. Noi: card_token -> silver_corebank.card. Cot PII: first_name, last_name, street, job, dob.
CREATE TABLE IF NOT EXISTS glue_catalog.silver_cards.card_authorization (
    `trans_num` string COMMENT 'Ma giao dich (khoa)',
    `trans_ts` timestamp COMMENT 'Thoi diem giao dich (gio dia phuong may phat)',
    `unix_time` bigint COMMENT 'Thoi diem dang epoch giay',
    `card_token` string COMMENT 'md5(so the) — noi silver_corebank.card',
    `cc_num_masked` string COMMENT 'So the da che (6 dau + 4 cuoi)',
    `merchant` string COMMENT 'Ten don vi chap nhan the',
    `category` string COMMENT 'Nhom nganh merchant: grocery_pos, shopping_net, kids_pets, ...',
    `amount` decimal(18,2) COMMENT 'So tien (CZK)',
    `first_name` string COMMENT 'PII: ten chu the',
    `last_name` string COMMENT 'PII: ho chu the',
    `gender` string COMMENT 'M | F',
    `street` string COMMENT 'PII: dia chi',
    `city` string COMMENT 'Thanh pho chu the',
    `state` string COMMENT 'Ma quoc gia/vung (CZ)',
    `zip` string COMMENT 'Ma buu chinh',
    `lat` double COMMENT 'Vi do chu the',
    `lon` double COMMENT 'Kinh do chu the',
    `city_pop` int COMMENT 'Dan so thanh pho chu the',
    `job` string COMMENT 'PII: nghe nghiep chu the',
    `dob` date COMMENT 'PII: ngay sinh chu the',
    `merch_lat` double COMMENT 'Vi do merchant',
    `merch_lon` double COMMENT 'Kinh do merchant',
    `txn_date` date COMMENT 'Ngay giao dich',
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
COMMENT 'Giao dich the (authorization) tu Kafka, append theo micro-batch'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);
