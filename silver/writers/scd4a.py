"""SCD4a snapshot — MỘT bảng chứa cả hiện hành lẫn lịch sử.

Bảng partition theo ds_partition_date; mỗi partition là ẢNH CHỤP ĐẦY ĐỦ của
ngày đó. Lần chạy cho ngày D ghi đè ĐÚNG partition D (dynamic overwrite) —
chạy lại ngày D bao nhiêu lần cũng ra cùng kết quả.

  "main"  = partition mới nhất:   WHERE ds_partition_date = (SELECT max(...))
  "hist"  = mọi partition cũ hơn.

ds_record_status so với ảnh chụp LIỀN TRƯỚC (ngày lớn nhất < D):

  I  key mới xuất hiện
  U  key có, ds_hash khác
  N  key có, không đổi
  D  key có ở ảnh trước, ảnh này không còn (emit_deletes) — dòng mang giá trị
     của ảnh trước, để truy vấn "hôm nay mất những ngân hàng nào" không cần join.

Đổi lấy sự đơn giản đó là dung lượng: mỗi ngày một bản đầy đủ. Với 27.834 ngân
hàng × 365 ngày ≈ 10 triệu dòng/năm — Iceberg + Parquet nén vẫn nhỏ. Bảng
hàng trăm triệu dòng mà ít đổi thì dùng SCD2.

GIỚI HẠN CÓ CHỦ Ý: một lô chỉ một ngày. Backfill ngày cũ làm status của các
ngày SAU nó thành cũ (so sánh với nền đã đổi) — chạy lại các ngày sau theo thứ tự.
"""

from __future__ import annotations

from typing import Dict

from .base import Writer


class Scd4aWriter(Writer):
    def write(self, df) -> Dict[str, int]:
        from pyspark.sql import functions as F

        dates = [r[0] for r in df.select("ds_partition_date").distinct().collect()]
        if not dates:
            return {"inserted": 0, "updated": 0, "deleted": 0, "unchanged": 0}
        if len(dates) != 1:
            raise RuntimeError(f"[{self.cfg.name}] scd4a nhan mot ngay moi lo, lo nay co "
                               f"{len(dates)} ngay: {sorted(dates)[:5]}")
        d = dates[0]

        prev_date = self.sql(
            f"SELECT max(ds_partition_date) FROM {self.table} WHERE ds_partition_date < DATE'{d}'"
        ).collect()[0][0]

        cols = df.columns
        if prev_date is None:
            snap = df   # anh dau tien: moi dong da la I (hoac D neu nguon bao xoa)
        else:
            prev = (self.spark.table(self.table)
                    .where(F.col("ds_partition_date") == F.lit(prev_date))
                    .where(F.col("ds_record_status") != "D"))
            p = prev.select(F.col("ds_key").alias("_pk"), F.col("ds_hash").alias("_ph"))
            cur = df.join(p, df.ds_key == p._pk, "left")
            snap = cur.withColumn(
                "ds_record_status",
                F.when(F.col("ds_record_status") == "D", "D")   # nguon bao xoa (delete_when)
                 .when(F.col("_pk").isNull(), "I")
                 .when(F.col("_ph") != F.col("ds_hash"), "U")
                 .otherwise("N")).select(*cols)

            if self.cfg.writer.emit_deletes:
                gone = (prev.join(df.select("ds_key"), "ds_key", "left_anti")
                        .withColumn("ds_record_status", F.lit("D"))
                        .withColumn("ds_partition_date", F.lit(d).cast("date"))
                        .withColumn("ds_etl_timestamp", F.current_timestamp())
                        .withColumn("ds_etl_job_id", F.lit(df.select("ds_etl_job_id").first()[0]))
                        .withColumn("ds_source", F.lit(self.source_label))
                        .select(*cols))
                snap = snap.unionByName(gone)

        # Chot du lieu truoc khi ghi — `snap` doc chinh bang dich (anh truoc).
        # Cung ly do voi SCD2: lazy + doc lai sau khi ghi = so lieu sai.
        snap = snap.localCheckpoint(eager=True)
        counts = self._counts_by_status(snap)
        snap.writeTo(self.table).overwritePartitions()
        counts["previous_snapshot"] = str(prev_date) if prev_date else ""
        return counts
