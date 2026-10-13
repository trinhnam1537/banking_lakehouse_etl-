"""SCD2 — mỗi thay đổi một phiên bản, khoảng hiệu lực [ds_valid_from, ds_valid_to).

Cột riêng:  ds_valid_from DATE, ds_valid_to DATE (9999-12-31 = hiện hành),
            ds_is_current BOOLEAN.
Ngày hiệu lực = ds_partition_date của lô (ngày dữ liệu, không phải giờ chạy).

Phân loại mỗi key (lô đã khử trùng lặp, so với phiên bản HIỆN HÀNH):

  mới                    -> chèn, status I
  đang ở trạng thái D    -> đóng bản D, chèn bản mới status I (tái xuất hiện)
  ds_hash khác           -> đóng bản cũ, chèn status U
  giống hệt              -> bỏ qua (không sinh phiên bản rác)
  nguồn báo xoá          -> đóng bản cũ, chèn bản "bia mộ" status D
  vắng mặt + snapshot    -> như nguồn báo xoá (chỉ khi writer.snapshot = true)

Tất cả gói trong MỘT câu MERGE (một commit Iceberg): người đọc không bao giờ
thấy trạng thái nửa vời "đã đóng bản cũ nhưng chưa có bản mới".

Mẹo MERGE kinh điển: mỗi thay đổi cần HAI hành động (UPDATE đóng bản cũ +
INSERT bản mới) mà một dòng nguồn chỉ khớp được một nhánh. Nên nhân đôi dòng:
một bản có `_merge_key = ds_key` (khớp -> đóng), một bản `_merge_key = NULL`
(không bao giờ khớp -> chèn).

CHẠY LẠI / THỨ TỰ:
  - Nguồn ẢNH CHỤP (snapshot: true): lô có ngày <= ngày đã có trong bảng ->
    HOÀN TÁC các phiên bản từ ngày đó trở đi rồi áp lại -> chạy lại ngày D cho
    cùng kết quả.
  - Nguồn DELTA (snapshot: false, CDC1): nhiều manifest trong một ngày chồng lên
    nhau; chỉ hoàn tác phiên bản của CHÍNH manifest đang chạy lại (ds_source).
    Đổi nhiều lần trong một ngày -> phiên bản [D, D) độ dài 0 cho các lần giữa.
  - Lô có ngày < ngày lớn nhất của bảng mà KHÔNG phải chạy lại (dữ liệu đến
    trễ sau khi ngày mới hơn đã ghi): TỪ CHỐI. Áp nó sẽ viết lại lịch sử mà
    các phiên bản sau đã dựa vào; phải backfill có chủ đích (--REBUILD_FROM).
"""

from __future__ import annotations

from typing import Dict

from ..transform import OPEN_END
from .base import Writer


class OutOfOrderError(RuntimeError):
    pass


class Scd2Writer(Writer):
    def _rollback_from(self, d) -> int:
        """Xoá mọi phiên bản bắt đầu từ ngày d, mở lại bản bị chúng đóng.

        Hai commit, không nguyên tử — nhưng idempotent: chết giữa chừng thì
        lần chạy sau làm lại đúng hai bước này.
        """
        n = self.sql(f"SELECT count(*) FROM {self.table} WHERE ds_valid_from >= DATE'{d}'").collect()[0][0]
        if n:
            self.sql(f"DELETE FROM {self.table} WHERE ds_valid_from >= DATE'{d}'")
            self.sql(f"""
                UPDATE {self.table}
                SET ds_valid_to = DATE'{OPEN_END}', ds_is_current = true
                WHERE ds_valid_to >= DATE'{d}' AND ds_valid_to <> DATE'{OPEN_END}'
            """)
        return n

    def _rollback_source(self, lo) -> int:
        """Hoàn tác các phiên bản do CHÍNH manifest này ghi (nguồn delta).

        Hoàn tác theo NGÀY như nguồn ảnh chụp là sai với delta: lô thứ hai trong
        ngày chỉ chứa những key vừa đổi, nên xoá cả ngày rồi áp lại sẽ làm mất
        thay đổi của lô thứ nhất. Ở đây chỉ xoá dòng có ds_source = manifest này,
        rồi mở lại phiên bản liền trước của đúng các key đó.

        Chỉ an toàn khi manifest này là lô CUỐI CÙNG đụng tới các key đó; có lô
        khác ghi sau thì từ chối, phải dựng lại có chủ đích (--REBUILD_FROM).
        """
        t, lbl = self.table, self._lit(self.source_label)
        mine = self.sql(f"""SELECT ds_key, min(ds_etl_timestamp) AS ts FROM {t}
                            WHERE ds_source = {lbl} GROUP BY ds_key""").localCheckpoint(eager=True)
        n = mine.count()
        if n == 0:
            return 0
        mine.createOrReplaceTempView("_x_scd2_mine")
        later = self.sql(f"""SELECT count(*) FROM {t} x JOIN _x_scd2_mine m ON x.ds_key = m.ds_key
                             WHERE x.ds_source <> {lbl} AND x.ds_etl_timestamp > m.ts""").collect()[0][0]
        if later:
            raise OutOfOrderError(
                f"[{self.cfg.name}] manifest {self.source_label} da duoc ap, sau do {later} phien ban "
                f"tu manifest khac da ghi len cung key. Khong hoan tac rieng duoc: dung lai bang "
                f"(--REBUILD_FROM {lo}) roi ap lai cac manifest theo thu tu.")
        self.sql(f"DELETE FROM {t} WHERE ds_source = {lbl}")
        self.sql(f"""
            MERGE INTO {t} x
            USING (
                SELECT ds_key, ds_valid_from, ds_etl_timestamp FROM (
                    SELECT y.ds_key, y.ds_valid_from, y.ds_etl_timestamp, y.ds_is_current,
                           row_number() OVER (PARTITION BY y.ds_key
                                              ORDER BY y.ds_valid_from DESC, y.ds_etl_timestamp DESC) AS rn
                    FROM {t} y JOIN _x_scd2_mine m ON y.ds_key = m.ds_key)
                WHERE rn = 1 AND NOT ds_is_current) s
            ON x.ds_key = s.ds_key AND x.ds_valid_from = s.ds_valid_from
               AND x.ds_etl_timestamp = s.ds_etl_timestamp
            WHEN MATCHED THEN UPDATE SET x.ds_valid_to = DATE'{OPEN_END}', x.ds_is_current = true
        """)
        return n

    def write(self, df, rebuild_from=None) -> Dict[str, int]:
        from pyspark.sql import functions as F

        rng = df.agg(F.min("ds_partition_date"), F.max("ds_partition_date")).collect()[0]
        lo, hi = rng[0], rng[1]
        if lo is None:
            return {"inserted": 0, "updated": 0, "deleted": 0, "unchanged": 0, "rolled_back": 0}
        if self.cfg.writer.snapshot and lo != hi:
            raise RuntimeError(f"[{self.cfg.name}] snapshot=true nhung lo co nhieu ngay {lo}..{hi}")

        table_max = self.sql(f"SELECT max(ds_valid_from) FROM {self.table}").collect()[0][0]
        rolled = 0
        if table_max is not None and table_max > hi and rebuild_from is None:
            raise OutOfOrderError(
                f"[{self.cfg.name}] lo ngay {lo}..{hi} den tre: bang da co phien ban ngay "
                f"{table_max}. Chay lai theo thu tu tu ngay {lo} (--REBUILD_FROM {lo}).")
        if rebuild_from is not None:
            rolled = self._rollback_from(rebuild_from)
        elif self.cfg.writer.snapshot:
            # Anh chup: lo cua ngay D THAY THE moi thu cua ngay D -> hoan tac ca ngay
            if table_max is not None and table_max >= lo:
                rolled = self._rollback_from(lo)
        else:
            # Delta (CDC1): nhieu manifest trong mot ngay CHONG len nhau. Chi hoan
            # tac dung manifest nay neu no da duoc ap (retry / chay lai tay).
            rolled = self._rollback_source(lo)

        cols = df.columns
        cur = (self.spark.table(self.table).where("ds_is_current")
               .select(*cols))

        inc = df.alias("i")
        how = "full_outer" if self.cfg.writer.snapshot else "left"
        j = inc.join(cur.alias("c"), F.col("i.ds_key") == F.col("c.ds_key"), how)

        i_, c_ = F.col("i.ds_key"), F.col("c.ds_key")
        c_status, i_status = F.col("c.ds_record_status"), F.col("i.ds_record_status")
        kind = (
            F.when(i_.isNull() & (c_status != "D"), "gone")                   # vang mat (snapshot)
             .when(i_.isNull(), "skip")                                        # vang + da D
             .when((i_status == "D") & c_.isNull(), "skip")                    # xoa key chua tung co
             .when((i_status == "D") & (c_status == "D"), "skip")              # xoa lan hai
             .when(i_status == "D", "delete")                                  # nguon bao xoa
             .when(c_.isNull(), "new")
             .when(c_status == "D", "reappear")
             .when(F.col("i.ds_hash") != F.col("c.ds_hash"), "change")
             .otherwise("same"))
        j = j.withColumn("_kind", kind)

        # Phien ban moi: lay gia tri cua lo; rieng "gone" lay gia tri ban hien hanh
        new_status = (F.when(F.col("_kind").isin("new", "reappear"), "I")
                       .when(F.col("_kind") == "change", "U").otherwise("D"))
        from_cur = F.col("_kind") == "gone"
        batch_date = F.lit(hi).cast("date")
        meta = df.select("ds_etl_job_id").first()[0]

        def pick(c):
            if c == "ds_record_status":
                return new_status.alias(c)
            if c == "ds_partition_date":
                return F.when(from_cur, batch_date).otherwise(F.col(f"i.{c}")).alias(c)
            if c == "ds_etl_timestamp":
                return F.current_timestamp().alias(c)
            if c == "ds_etl_job_id":
                return F.lit(meta).alias(c)
            if c == "ds_source":
                return F.lit(self.source_label).alias(c)
            return F.when(from_cur, F.col(f"c.{c}")).otherwise(F.col(f"i.{c}")).alias(c)

        # localCheckpoint (EAGER) chu khong cache (lazy): `changes` doc chinh
        # bang dich. Neu de lazy, moi lan dung lai SAU MERGE no se tinh lai tren
        # bang DA doi -> dong vua chen bi xep thanh "same", so lieu ra 0.
        # (Bug nay test da bat duoc: inserted=0 o lan nap dau.)
        changes = (j.where(~F.col("_kind").isin("same", "skip"))
                   .select(*[pick(c) for c in cols],
                           (F.col("_kind") != "new").alias("_close"))
                   .withColumn("ds_valid_from", F.col("ds_partition_date"))
                   .withColumn("ds_valid_to", F.lit(OPEN_END).cast("date"))
                   .withColumn("ds_is_current", F.lit(True))
                   .localCheckpoint(eager=True))
        counts = self._counts_by_status(changes)
        counts["unchanged"] = j.where("_kind = 'same'").count()
        counts["rolled_back"] = rolled

        staged = (changes.withColumn("_merge_key", F.lit(None).cast("string"))
                  .unionByName(changes.where("_close").withColumn("_merge_key", F.col("ds_key"))))
        staged.drop("_close").createOrReplaceTempView("_x_scd2_staged")

        all_cols = cols + ["ds_valid_from", "ds_valid_to", "ds_is_current"]
        col_list = ", ".join(f"`{c}`" for c in all_cols)
        val_list = ", ".join(f"s.`{c}`" for c in all_cols)
        self.sql(f"""
            MERGE INTO {self.table} t
            USING _x_scd2_staged s
            ON t.ds_key = s._merge_key AND t.ds_is_current = true
            WHEN MATCHED THEN UPDATE SET
                t.ds_valid_to = s.ds_valid_from,
                t.ds_is_current = false
            WHEN NOT MATCHED AND s._merge_key IS NULL THEN
                INSERT ({col_list}) VALUES ({val_list})
        """)

        return counts
