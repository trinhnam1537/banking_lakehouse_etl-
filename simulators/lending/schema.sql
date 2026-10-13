-- "Hệ thống cho vay" giả lập theo Home Credit Default Risk (Kaggle 2018), 7 bảng.
-- Nạp: simulators/lending/seed.py (file Kaggle nếu có, không thì sinh tổng hợp cùng schema).
-- Sinh mới: simulators/lending/simulate.py.
--
-- Những gì khác bản Kaggle, và vì sao (tầng sourcing cần):
--   * Bảng con không có khoá đơn (bureau_balance, pos_cash_balance, credit_card_balance,
--     installments_payments có dòng TRÙNG) -> thêm `id bigserial`: vừa là khoá, vừa là
--     mốc CDC1 chia khúc (reader JDBC chỉ nhận MỘT cột mốc).
--   * created_at / updated_at + trigger cho bảng bị sửa (application, bureau,
--     previous_application) -> mốc CDC1 theo thời gian.
--   * application.customer_ref = core.client.client_id: hồ sơ vay nối về khách core banking
--     (bảng ánh xạ GIẢ LẬP, ghi rõ trong docs/sources/lending.md).
--   * application_date: ngày nộp thật; các cột DAYS_* giữ nguyên nghĩa "số ngày tính tới
--     ngày nộp" như nguồn (silver đổi thành ngày tuyệt đối).
--   * TARGET (vỡ nợ) KHÔNG nằm trong hệ thống nguồn: là nhãn đến sau, simulator ghi
--     thành file nhãn riêng (landing/_inbox/lending/labels/).
--   * Cột ít dùng của application (122 cột gốc) nằm trong `extra jsonb` thay vì mất đi.

CREATE SCHEMA IF NOT EXISTS lending;

CREATE OR REPLACE FUNCTION lending.touch_updated_at() RETURNS trigger AS $$
BEGIN
  NEW.updated_at := clock_timestamp();
  RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TABLE lending.application (
  sk_id_curr          bigint PRIMARY KEY,
  customer_ref        integer,                 -- core.client.client_id (giả lập)
  application_date    date NOT NULL,
  status              text NOT NULL,           -- SUBMITTED | APPROVED | REJECTED | DISBURSED
  name_contract_type  text,                    -- Cash loans | Revolving loans
  code_gender         text,
  flag_own_car        text,
  flag_own_realty     text,
  cnt_children        integer,
  amt_income_total    numeric(14,2),
  amt_credit          numeric(14,2),
  amt_annuity         numeric(14,2),
  amt_goods_price     numeric(14,2),
  name_income_type    text,
  name_education_type text,
  name_family_status  text,
  name_housing_type   text,
  days_birth          integer,
  days_employed       integer,                 -- 365243 = mã "không đi làm/hưu" của nguồn
  occupation_type     text,
  organization_type   text,
  ext_source_1        numeric(8,6),
  ext_source_2        numeric(8,6),
  ext_source_3        numeric(8,6),
  extra               jsonb,
  created_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at          timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Lịch sử tín dụng ở tổ chức khác (tương đương CIC)
CREATE TABLE lending.bureau (
  sk_id_bureau           bigint PRIMARY KEY,
  sk_id_curr             bigint NOT NULL REFERENCES lending.application,
  credit_active          text,
  credit_currency        text,
  days_credit            integer,
  credit_day_overdue     integer,
  days_credit_enddate    integer,
  amt_credit_sum         numeric(16,2),
  amt_credit_sum_debt    numeric(16,2),
  amt_credit_sum_overdue numeric(16,2),
  credit_type            text,
  days_credit_update     integer,
  amt_annuity            numeric(14,2),
  created_at             timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at             timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE lending.bureau_balance (
  id             bigserial PRIMARY KEY,
  sk_id_bureau   bigint NOT NULL REFERENCES lending.bureau,
  months_balance integer NOT NULL,
  status         text,                          -- C đóng | X không rõ | 0..5 số kỳ quá hạn
  created_at     timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE lending.previous_application (
  sk_id_prev           bigint PRIMARY KEY,
  sk_id_curr           bigint NOT NULL REFERENCES lending.application,
  name_contract_type   text,
  amt_annuity          numeric(14,2),
  amt_application      numeric(14,2),
  amt_credit           numeric(14,2),
  amt_goods_price      numeric(14,2),
  name_contract_status text,                    -- Approved | Refused | Canceled | Unused offer
  days_decision        integer,
  name_payment_type    text,
  code_reject_reason   text,
  name_client_type     text,
  cnt_payment          integer,
  name_yield_group     text,
  created_at           timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at           timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE lending.pos_cash_balance (
  id                    bigserial PRIMARY KEY,
  sk_id_prev            bigint NOT NULL,
  sk_id_curr            bigint NOT NULL,
  months_balance        integer NOT NULL,
  cnt_instalment        integer,
  cnt_instalment_future integer,
  name_contract_status  text,
  sk_dpd                integer,
  sk_dpd_def            integer,
  created_at            timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE lending.credit_card_balance (
  id                      bigserial PRIMARY KEY,
  sk_id_prev              bigint NOT NULL,
  sk_id_curr              bigint NOT NULL,
  months_balance          integer NOT NULL,
  amt_balance             numeric(14,2),
  amt_credit_limit_actual numeric(14,2),
  amt_drawings_current    numeric(14,2),
  amt_payment_current     numeric(14,2),
  amt_total_receivable    numeric(14,2),
  cnt_drawings_current    integer,
  name_contract_status    text,
  sk_dpd                  integer,
  created_at              timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE lending.installments_payments (
  id                     bigserial PRIMARY KEY,
  sk_id_prev             bigint NOT NULL,
  sk_id_curr             bigint NOT NULL,
  num_instalment_version numeric(6,1),
  num_instalment_number  integer,
  days_instalment        numeric(10,1),
  days_entry_payment     numeric(10,1),
  amt_instalment         numeric(14,3),
  amt_payment            numeric(14,3),
  created_at             timestamptz NOT NULL DEFAULT clock_timestamp()
);

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['application','bureau','previous_application'] LOOP
    EXECUTE format('CREATE TRIGGER %I_touch BEFORE UPDATE ON lending.%I
                    FOR EACH ROW EXECUTE FUNCTION lending.touch_updated_at()', t, t);
    EXECUTE format('CREATE INDEX %I_updated_at ON lending.%I (updated_at)', t, t);
  END LOOP;
END $$;

CREATE ROLE etl_reader LOGIN PASSWORD 'etl_reader';
GRANT USAGE ON SCHEMA lending TO etl_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA lending TO etl_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA lending GRANT SELECT ON TABLES TO etl_reader;
