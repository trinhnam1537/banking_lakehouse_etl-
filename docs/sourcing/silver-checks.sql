-- Kiem tra silver sau lan nap 02-11-2026. Chay trong Athena, workgroup fingraph-dev-data-engineer,
-- bang persona DE (TerraformAdmin la admin Lake Formation nhung KHONG co quyen doc du lieu).
-- So ky vong lay tu metrics slv-job-log cua lan chay do.

-- 1. IBM AML: 16 ngay, ~2 trieu giao dich/ngay; tong 31.891.251
SELECT event_date, count(*) AS n,
       sum(CASE WHEN is_self_transfer THEN 1 ELSE 0 END) AS self_transfer,
       sum(CASE WHEN is_cross_currency THEN 1 ELSE 0 END) AS cross_currency
FROM silver_aml.transfer GROUP BY 1 ORDER BY 1;

-- 2. IBM AML: ty le rua tien (ky vong ~1/905) — noi transfer voi label qua txn_id
SELECT count(*) AS n, sum(CASE WHEN l.is_laundering THEN 1 ELSE 0 END) AS laundering,
       count(l.txn_id) AS co_nhan
FROM silver_aml.transfer t LEFT JOIN silver_aml.label l ON l.txn_id = t.txn_id;

-- 3. IBM AML: kieu hinh rua tien, so vu va so giao dich khop duoc voi transfer
--    (giao dich cua vu nam o 12 ngay duoi da bo se khong khop)
SELECT a.typology, count(DISTINCT a.attempt_id) AS vu, count(*) AS gd, count(t.txn_id) AS khop_transfer
FROM silver_aml.laundering_attempt a LEFT JOIN silver_aml.transfer t ON t.txn_id = a.txn_id
GROUP BY 1 ORDER BY 2 DESC;

-- 4. IBM AML: tai khoan theo loai phap nhan; moi giao dich deu tim thay tai khoan gui
SELECT entity_type, count(*) FROM silver_aml.account GROUP BY 1 ORDER BY 2 DESC;
SELECT count(*) AS thieu_tai_khoan_gui FROM silver_aml.transfer t
LEFT JOIN silver_aml.account a ON a.bank_id = t.from_bank AND a.account_number = t.from_account
WHERE a.account_number IS NULL;

-- 5. PaySim: gian lan chi o TRANSFER va CASH_OUT (dac diem da biet cua bo du lieu), tong 8.213
SELECT t.txn_type, count(*) AS n, sum(CASE WHEN l.is_fraud THEN 1 ELSE 0 END) AS fraud
FROM silver_paysim.transaction t JOIN silver_paysim.fraud_label l ON l.txn_ref = t.txn_ref
GROUP BY 1 ORDER BY 1;

-- 6. Bank Marketing: ty le dang ky ~11,7% (full), ~11,3% (additional)
SELECT 'full' AS bo, count(*), avg(CASE WHEN subscribed THEN 1.0 ELSE 0 END) FROM silver_bank_marketing.contact_full
UNION ALL
SELECT 'additional', count(*), avg(CASE WHEN subscribed THEN 1.0 ELSE 0 END) FROM silver_bank_marketing.contact_additional;

-- 7. MBD: 12 thang bao cao, ~100 nghin khach/thang
SELECT report_month, count(*), avg(CASE WHEN target_1 THEN 1.0 ELSE 0 END) AS p_target_1
FROM silver_mbd.target_month GROUP BY 1 ORDER BY 1;

-- 8. The <-> core banking qua card_token (lien ket gia lap, ky vong ~100% khop)
SELECT count(*) AS giao_dich, count(c.card_token) AS khop_corebank
FROM silver_cards.card_authorization a
LEFT JOIN (SELECT DISTINCT card_token FROM silver_corebank.card) c ON c.card_token = a.card_token;

-- 9. FX: ty gia USD gan nhat
SELECT max_by(usd_czk, rate_date) AS usd_czk, max(rate_date) FROM silver_fx.cnb_czk_rate;
SELECT rate_date, buy_transfer, sell FROM silver_fx.vcb_vnd_rate WHERE currency_code = 'USD' ORDER BY rate_date DESC LIMIT 3;

-- 10. OpenSanctions: doi tuong hien hanh theo loai; su kien delta theo op
SELECT entity_schema, count(*) FROM silver_opensanctions.target
WHERE ds_is_current AND ds_record_status <> 'D' GROUP BY 1 ORDER BY 2 DESC;
SELECT op, count(*) FROM silver_opensanctions.entity_change GROUP BY 1;
