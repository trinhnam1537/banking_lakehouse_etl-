-- CHỈ cho stack local: user chỉ đọc của ETL với mật khẩu dễ nhớ.
-- Chạy TRƯỚC simulators/corebank/schema.sql (tên file 05- < 10-), vì schema GRANT cho role này.
-- Trên AWS role do máy nguồn tạo, mật khẩu ngẫu nhiên (repo hạ tầng, corebank-init.sh).
CREATE ROLE etl_reader LOGIN PASSWORD 'etl_reader';
