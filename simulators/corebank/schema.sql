-- "Core banking" giả lập: dữ liệu Berka (ngân hàng Séc 1993-1998, dời +24 năm)
-- cộng simulator sinh giao dịch mới từ 2023-01-01 (simulators/corebank/simulate.py).
--
-- Khác bản gốc CTU ở những chỗ một hệ thống nguồn thật PHẢI có, và tầng sourcing
-- dựa vào:
--   * created_at / updated_at (trigger tự cập nhật)  -> mốc CDC1 cho bảng bị SỬA
--   * trans_id tăng dần, trans chỉ INSERT (append)    -> CDC1 chia khúc theo trans_id
--   * xoá mềm (status), không DELETE                   -> CDC1 không mất bản ghi xoá
--   * tên cột đọc được thay cho A2..A16, mã tiếng Séc giữ nguyên (silver dịch)
--   * họ tên / số định danh giả lập cho client          -> sàng lọc cấm vận có cái để so
--
-- File này được Postgres chạy MỘT lần khi khởi tạo volume (docker-entrypoint-initdb.d).

CREATE SCHEMA IF NOT EXISTS core;

CREATE OR REPLACE FUNCTION core.touch_updated_at() RETURNS trigger AS $$
BEGIN
  NEW.updated_at := clock_timestamp();
  RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TABLE core.district (
  district_id            integer PRIMARY KEY,
  district_name          text NOT NULL,
  region                 text NOT NULL,
  inhabitants            integer,
  munic_lt_500           integer,
  munic_500_1999         integer,
  munic_2000_9999        integer,
  munic_ge_10000         integer,
  cities                 integer,
  urban_ratio            numeric(5,1),
  avg_salary             integer,
  unemployment_95        numeric(5,2),
  unemployment_96        numeric(5,2),
  entrepreneurs_per_1000 integer,
  crimes_95              integer,
  crimes_96              integer,
  created_at             timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at             timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE core.client (
  client_id     integer PRIMARY KEY,
  full_name     text NOT NULL,          -- GIẢ LẬP (Faker cs_CZ), Berka đã ẩn danh
  national_id   text NOT NULL UNIQUE,   -- GIẢ LẬP, dạng rodné číslo YYMMDD/XXXX
  gender        char(1) NOT NULL,       -- F | M
  birth_date    date NOT NULL,
  district_id   integer NOT NULL REFERENCES core.district,
  email         text,
  phone         text,
  address       text,
  status        text NOT NULL DEFAULT 'ACTIVE',   -- ACTIVE | CLOSED (xoá mềm)
  created_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at    timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE core.account (
  account_id    integer PRIMARY KEY,
  district_id   integer NOT NULL REFERENCES core.district,
  frequency     text NOT NULL,          -- POPLATEK MESICNE | POPLATEK TYDNE | POPLATEK PO OBRATU
  opened_date   date NOT NULL,
  currency      char(3) NOT NULL DEFAULT 'CZK',
  balance       numeric(18,2) NOT NULL DEFAULT 0,
  status        text NOT NULL DEFAULT 'ACTIVE',
  created_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at    timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE core.disp (
  disp_id       integer PRIMARY KEY,
  client_id     integer NOT NULL REFERENCES core.client,
  account_id    integer NOT NULL REFERENCES core.account,
  disp_type     text NOT NULL,          -- OWNER | DISPONENT
  created_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at    timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE core.card (
  card_id       integer PRIMARY KEY,
  disp_id       integer NOT NULL REFERENCES core.disp,
  card_type     text NOT NULL,          -- classic | junior | gold
  issued_date   date NOT NULL,
  card_number   text NOT NULL UNIQUE,   -- GIẢ LẬP 16 số (simulator thẻ dùng làm cc_num)
  status        text NOT NULL DEFAULT 'ACTIVE',
  created_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at    timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE core.loan (
  loan_id       integer PRIMARY KEY,
  account_id    integer NOT NULL REFERENCES core.account,
  granted_date  date NOT NULL,
  amount        numeric(18,2) NOT NULL,
  duration      integer NOT NULL,       -- tháng
  payments      numeric(18,2) NOT NULL, -- trả hằng tháng
  status        char(1) NOT NULL,       -- A xong-đủ | B xong-thiếu | C đang-đúng hạn | D đang-quá hạn
  created_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at    timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE core.standing_order (
  order_id      integer PRIMARY KEY,
  account_id    integer NOT NULL REFERENCES core.account,
  bank_to       text NOT NULL,
  account_to    bigint NOT NULL,
  amount        numeric(12,2) NOT NULL,
  k_symbol      text,                   -- SIPO | UVER | POJISTNE | LEASING | NULL
  active        boolean NOT NULL DEFAULT true,
  created_at    timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at    timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Sổ giao dịch: CHỈ INSERT. Sửa sai bằng bút toán đảo, không UPDATE.
CREATE TABLE core.trans (
  trans_id      bigint PRIMARY KEY,
  account_id    integer NOT NULL REFERENCES core.account,
  trans_date    date NOT NULL,
  booked_at     timestamptz NOT NULL,
  trans_type    text NOT NULL,          -- PRIJEM (có) | VYDAJ (nợ) | VYBER (lỗi gốc Berka)
  operation     text,
  amount        numeric(18,2) NOT NULL,
  balance       numeric(18,2) NOT NULL,
  k_symbol      text,
  bank          text,
  counter_account bigint,
  created_at    timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- Bản ghi mới của simulator lấy id tiếp sau id lớn nhất của Berka (3.682.987)
CREATE SEQUENCE core.trans_id_seq START 4000000 OWNED BY core.trans.trans_id;
ALTER TABLE core.trans ALTER COLUMN trans_id SET DEFAULT nextval('core.trans_id_seq');

CREATE INDEX trans_account_date ON core.trans (account_id, trans_date);

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['district','client','account','disp','card','loan','standing_order'] LOOP
    EXECUTE format('CREATE TRIGGER %I_touch BEFORE UPDATE ON core.%I
                    FOR EACH ROW EXECUTE FUNCTION core.touch_updated_at()', t, t);
    -- Index cho CDC1: WHERE updated_at > :watermark ORDER BY updated_at
    EXECUTE format('CREATE INDEX %I_updated_at ON core.%I (updated_at)', t, t);
  END LOOP;
END $$;

-- User CHỈ ĐỌC cho ETL (secret fingraph/src/corebank_pg). Không dùng user chủ DB.
-- Role PHẢI có sẵn trước file này, và mật khẩu KHÔNG nằm ở đây:
--   local: local/initdb/corebank-roles.sql (etl_reader/etl_reader, chỉ cho máy dev)
--   AWS:   máy nguồn tự sinh mật khẩu (repo hạ tầng modules/kafka/scripts/corebank-init.sh)
GRANT USAGE ON SCHEMA core TO etl_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA core TO etl_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA core GRANT SELECT ON TABLES TO etl_reader;
