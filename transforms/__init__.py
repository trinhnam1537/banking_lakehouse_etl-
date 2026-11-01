"""Tầng gold và pre-train — job PySpark TỰ VIẾT logic (không phải entry YAML).

    silver_*  (Iceberg, framework YAML)  ──gold-spark──>  track_a_gold      (bảng nghiệp vụ, BA/DA đọc)
                                          ──pretrain-spark──> track_a_pretrain (bộ dữ liệu huấn luyện)

Vì sao không YAML như sourcing/silver: hai tầng trên là logic nghiệp vụ — lưới khách × tháng,
cửa sổ trượt, nhãn theo tương lai, chia tập theo thời gian. Diễn đạt bằng khai báo sẽ thành
một ngôn ngữ lập trình nửa vời; viết thẳng PySpark thì đọc, test và review như code thường.

Mỗi tầng một Glue job riêng (repo hạ tầng: modules/transforms), tham số --TRANSFORM chọn
logic nào chạy. Thêm logic mới = thêm một module có hàm `run(spark, args)` + một dòng ở
`main.REGISTRY`, không đụng hạ tầng.
"""
