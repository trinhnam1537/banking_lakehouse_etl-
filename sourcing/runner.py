"""Luồng chung source → raw.

    đọc cấu hình (DynamoDB hoặc YAML)
      → mở nhật ký, ghi RUN#... trạng thái RUNNING
        → đọc checkpoint (CKPT#LATEST)
          → reader.read() sinh từng lô
            → ghi lô xuống S3
            → gom mốc; sau mỗi N lô ghi ảnh chụp PARTIAL
      → ghi SUCCESS + tiến mốc lần cuối, hoặc FAILED

Hai bất biến của tầng này:

**Một — ghi dữ liệu trước, tiến mốc sau.** Đảo lại thì job chết giữa chừng sẽ
bỏ sót đúng khoảng vừa đọc mà chưa kịp lưu, và không có gì báo lỗi.

**Hai — mốc chỉ tiến khi trạng thái là PARTIAL hoặc SUCCESS.** Lần chạy
FAILED không bao giờ đẩy mốc bằng phần nó đọc dở. Nhưng ảnh chụp PARTIAL
trong cùng lần chạy đó thì có — nên job gặp backlog lớn hơn timeout vẫn nhích
dần lên qua từng lần chạy thay vì lặp vô hạn ở một chỗ.

Hệ quả có chủ ý: ngữ nghĩa at-least-once. Job chết giữa hai ảnh chụp sẽ đọc
lại phần chồng lấn ở lần sau. Tầng bronze phải khử trùng lặp theo khoá nghiệp
vụ — rẻ và dễ kiểm chứng hơn nhiều so với giao dịch hai pha giữa S3 và DynamoDB.
"""

from __future__ import annotations

import time
import traceback
from dataclasses import dataclass, field
from typing import Dict, Optional

from .config import CdcMode, SourceConfig
from .readers import build_reader
from .state import PARTIAL_EVERY_N_BATCHES, Checkpoint, JobLogStore, RunStats
from .writer import RawWriter


@dataclass
class RunResult:
    job_name: str
    run_id: str
    status: str
    stats: RunStats
    watermarks: Dict[str, str] = field(default_factory=dict)
    error: str = ""

    @property
    def ok(self) -> bool:
        return self.status in ("SUCCESS", "NO_DATA")


class SourceRunner:
    def __init__(
        self,
        cfg: SourceConfig,
        raw_bucket: str,
        joblog_table: str,
        run_id: Optional[str] = None,
        dry_run: bool = False,
    ):
        self.cfg = cfg
        self.raw_bucket = raw_bucket
        self.dry_run = dry_run
        self.log = JobLogStore(joblog_table)
        # Step Functions sinh run_id rồi truyền vào, để bản ghi mà SFN cập
        # nhật lúc kết thúc trùng đúng bản ghi mà job đã tạo lúc bắt đầu.
        self.run_id = run_id or JobLogStore.new_run_id()

    def _config_summary(self) -> dict:
        """Ảnh chụp cấu hình lưu kèm nhật ký.

        Khi một lần chạy cũ cho kết quả lạ, câu hỏi đầu tiên luôn là "lúc đó
        cấu hình thế nào". Lưu sẵn thì trả lời được, kể cả sau khi cấu hình
        đã đổi nhiều lần.
        """
        return {
            "source_type": self.cfg.source_type.value,
            "cdc_mode": self.cfg.cdc.mode.value,
            "watermark_column": self.cfg.cdc.watermark_column or "",
            "lookback_seconds": self.cfg.cdc.lookback_seconds,
            "raw_path": self.cfg.raw_path,
            "engine": self.cfg.runtime.engine.value,
        }

    def _raw_mode(self) -> str:
        """Hình dạng dòng raw, để silver biết cách bóc mà không phải đoán."""
        if self.cfg.api is not None:
            return self.cfg.api.raw_mode
        return {"kafka": "kafka_message", "jdbc": "records",
                "s3file": "records"}.get(self.cfg.source_type.value, "records")

    def run(self) -> RunResult:
        stats = RunStats()
        t0 = time.monotonic()
        pending: Dict[str, str] = {}  # moc da doc nhung chua ghi anh chup

        run_key = self.log.start_run(self.cfg.name, self.run_id, self._config_summary())
        print(f"[{self.cfg.name}] run_id={self.run_id} "
              f"mode={self.cfg.cdc.mode.value} type={self.cfg.source_type.value}")

        writer = RawWriter(self.cfg, self.raw_bucket, self.run_id)

        try:
            checkpoint = Checkpoint(job_name=self.cfg.name)
            if self.cfg.cdc.mode is CdcMode.CDC1:
                checkpoint = self.log.read_checkpoint(self.cfg.name)
                print(f"[{self.cfg.name}] moc truoc: "
                      f"{checkpoint.watermarks or '(chua co)'} "
                      f"(tu {checkpoint.source_status or 'khong co'} "
                      f"luc {checkpoint.updated_at or '-'})")

            with build_reader(self.cfg, checkpoint=checkpoint) as reader:
                for batch in reader.read(checkpoint):
                    stats.records_read += len(batch)
                    stats.batches += 1
                    if batch.meta.get("page"):
                        stats.pages_fetched = max(stats.pages_fetched, batch.meta["page"])

                    if self.dry_run:
                        print(f"[{self.cfg.name}] DRY-RUN: bo qua {len(batch)} ban ghi "
                              f"(partition={batch.partition})")
                        continue

                    writer.write(batch.records)

                    if batch.watermark and self.cfg.cdc.mode is CdcMode.CDC1:
                        pending[batch.partition] = batch.watermark

                    # Ảnh chụp định kỳ. Đây là thứ giữ cho job backlog lớn
                    # không lặp vô hạn ở cùng một chỗ.
                    if pending and stats.batches % PARTIAL_EVERY_N_BATCHES == 0:
                        self.log.advance_checkpoint(
                            self.cfg.name, self.run_id, pending, "PARTIAL"
                        )
                        print(f"[{self.cfg.name}] PARTIAL sau {stats.batches} lo: {pending}")
                        pending = {}

            stats.records_written = writer.result.records
            stats.bytes_written = writer.result.bytes_out
            stats.files_written = writer.result.files

            status = "SUCCESS" if stats.records_read else "NO_DATA"

            # Mốc cuối cùng chỉ được ghi Ở ĐÂY, sau khi toàn bộ dữ liệu đã
            # nằm trên S3 và không còn ngoại lệ nào có thể xảy ra.
            if pending and not self.dry_run:
                self.log.advance_checkpoint(self.cfg.name, self.run_id, pending, "SUCCESS")

            # Manifest SAU moc: no la trigger cua silver. Neu ghi manifest loi
            # thi lan chay tinh la FAILED (silver se khong bao gio thay du lieu
            # nay); du lieu + moc da ghi van hop le, lan sau keo chong -> silver
            # khu trung lap theo ds_key.
            mkey = None
            if not self.dry_run:
                mkey = writer.write_manifest(status, {"raw_mode": self._raw_mode()})
                if mkey:
                    print(f"[{self.cfg.name}] manifest s3://{self.raw_bucket}/{mkey}")

            # Bao cho orchestrator (neu lan chay do no kich). Loi o day KHONG
            # lam hong lan chay: du lieu + moc + manifest da xong, cai mat di
            # chi la event noi sang silver — chay lai silver bang tay duoc.
            try:
                self.log.record_orch_result(self.cfg.name, self.run_id,
                                            "DRY_RUN" if self.dry_run else status, mkey)
            except Exception as e:  # noqa: BLE001
                print(f"[{self.cfg.name}] CANH BAO: khong ghi duoc ORCH#{self.run_id}: {e}")

            final = self.log.read_checkpoint(self.cfg.name).watermarks
            self.log.finish_run(self.cfg.name, run_key, status, stats, watermarks=final)

            print(f"[{self.cfg.name}] {status} doc={stats.records_read} "
                  f"ghi={stats.records_written} file={stats.files_written} "
                  f"lo={stats.batches} {time.monotonic() - t0:.1f}s")
            return RunResult(self.cfg.name, self.run_id, status, stats, final)

        except Exception as e:  # noqa: BLE001
            err = f"{type(e).__name__}: {e}\n{traceback.format_exc()[-2000:]}"
            stats.records_written = writer.result.records
            stats.bytes_written = writer.result.bytes_out
            stats.files_written = writer.result.files

            # KHÔNG ghi `pending` vào mốc. Phần đọc dở của một lần chạy hỏng
            # không được tính là đã xử lý xong — trừ những ảnh chụp PARTIAL
            # đã ghi trước đó, vốn ứng với dữ liệu đã nằm an toàn trên S3.
            partial = self.log.read_checkpoint(self.cfg.name).watermarks
            self.log.finish_run(self.cfg.name, run_key, "FAILED", stats,
                                watermarks=partial, error=err)

            print(f"[{self.cfg.name}] FAILED sau {time.monotonic() - t0:.1f}s: {e}")
            print(f"[{self.cfg.name}] moc giu nguyen o: {partial or '(chua co)'}")
            raise
