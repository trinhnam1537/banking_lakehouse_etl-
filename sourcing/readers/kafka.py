"""Reader cho Kafka / Redpanda, đọc theo lô.

Khác biệt cơ bản so với API và JDBC: mốc ở đây là **offset từng partition**,
không phải một cột dữ liệu. Một topic 12 partition sinh ra 12 dòng checkpoint
độc lập, nên tiến độ mỗi partition không chặn nhau.

Vì sao đọc theo lô chứ không streaming: Glue Streaming tính tối thiểu 2 DPU
chạy liên tục, khoảng 20 USD/ngày. Job Python Shell chạy 5 phút một lần tốn
gần như bằng không, đổi lại độ trễ vài phút. Với tầng raw thì đánh đổi đó là
đúng — xem bảng 2.2 tài liệu thiết kế.

CDC5 không có nghĩa với Kafka (không thể "snapshot toàn bộ" một topic có
retention), nên reader này luôn chạy như CDC1.
"""

from __future__ import annotations

from typing import Dict, Iterator, List, Optional

from ..config import SourceConfig
from ..state import Checkpoint
from .base import Batch, Reader

BATCH_SIZE = 10_000


def sasl_conf(username: str, password: str, mechanism: str = "SCRAM-SHA-512") -> Dict[str, str]:
    """Cấu hình librdkafka cho SASL/SCRAM. User rỗng hoặc còn nguyên `${BIEN}` -> {} (PLAINTEXT).

    `${BIEN}` còn nguyên nghĩa là máy chạy không đặt biến: chạy local với Redpanda không
    xác thực. Gặp broker có SASL thì broker từ chối kết nối, lỗi hiện ngay ở lần đầu.
    """
    if not username or username.startswith("${"):
        return {}
    if not password or password.startswith("${"):
        raise ValueError(f"co sasl_username '{username}' nhung thieu mat khau")
    return {
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": mechanism,
        "sasl.username": username,
        "sasl.password": password,
    }


class KafkaReader(Reader):
    """Cần `confluent-kafka` — khai trong runtime.additional_python_modules."""

    def __init__(self, cfg: SourceConfig, checkpoint: Optional[Checkpoint] = None):
        super().__init__(cfg)
        if cfg.kafka is None:
            raise ValueError(f"[{cfg.name}] thieu khoi cau hinh 'kafka'")
        self.k = cfg.kafka
        self._seed = checkpoint
        self._consumer = None

    def _build(self):
        from confluent_kafka import Consumer  # noqa: PLC0415

        return Consumer({
            "bootstrap.servers": self.k.bootstrap_servers,
            "group.id": self.k.group_id,
            "auto.offset.reset": self.k.auto_offset_reset,
            # TẮT auto-commit. Offset chỉ được ghi sau khi dữ liệu đã nằm trên
            # S3 — commit tự động theo thời gian sẽ báo "đã xử lý" cho những
            # bản tin còn nằm trong bộ nhớ lúc job chết.
            "enable.auto.commit": False,
            "socket.timeout.ms": 20_000,
            **sasl_conf(self.k.sasl_username, self.k.sasl_password, self.k.sasl_mechanism),
        })

    def read(self, checkpoint: Optional[Checkpoint]) -> Iterator[Batch]:
        from confluent_kafka import KafkaError, TopicPartition  # noqa: PLC0415

        self._consumer = self._build()

        # Lấy danh sách partition rồi tua tới offset đã lưu. Không dùng
        # subscribe(): consumer group tự cân bằng sẽ giành quyền quyết định
        # offset, còn ở đây mình muốn kiểm soát hoàn toàn từ DynamoDB.
        md = self._consumer.list_topics(self.k.topic, timeout=20)
        if self.k.topic not in md.topics or md.topics[self.k.topic].error:
            raise RuntimeError(
                f"[{self.cfg.name}] khong thay topic '{self.k.topic}' tren "
                f"{self.k.bootstrap_servers}"
            )
        part_ids = sorted(md.topics[self.k.topic].partitions.keys())

        assignments: List[TopicPartition] = []
        # Moc dung cua lan chay: high-watermark tung partition CHUP LUC BAT DAU. Producer chay
        # lien tuc (simulators.cards.stream) thi `consume` khong bao gio tra rong -> khong co
        # moc nay, mot "micro-batch" doc toi max_records_per_run (500k tin ~ 70 gio o 2 tin/s,
        # su co 10-31). Tin den sau moc thuoc lan chay sau.
        end: Dict[int, int] = {}
        start_at: Dict[int, int] = {}
        for pid in part_ids:
            source = checkpoint or self._seed
            saved = source.get(str(pid)) if source else None
            if saved is not None:
                # +1: offset đã lưu là bản tin CUỐI đã xử lý, không phải bản
                # tin kế tiếp. Quên +1 là đọc lại một bản tin mỗi lần chạy.
                assignments.append(TopicPartition(self.k.topic, pid, int(saved) + 1))
                start_at[pid] = int(saved) + 1
            else:
                from confluent_kafka import OFFSET_BEGINNING, OFFSET_END  # noqa: PLC0415

                start = (OFFSET_BEGINNING if self.k.auto_offset_reset == "earliest"
                         else OFFSET_END)
                assignments.append(TopicPartition(self.k.topic, pid, start))
            # latest + chua co moc: khong chup (chup thi lan nao cung bat dau tu cuoi -> khong bao
            # gio co tin, khong bao gio co moc). Giu hanh vi cu: doc toi khi het tin.
            if saved is not None or self.k.auto_offset_reset == "earliest":
                low, high = self._consumer.get_watermark_offsets(
                    TopicPartition(self.k.topic, pid, 0), timeout=20)
                end[pid] = high
                start_at.setdefault(pid, low)
        self._consumer.assign(assignments)

        done = {pid for pid in end if start_at[pid] >= end[pid]}
        bounded_only = len(end) == len(part_ids)
        if bounded_only and len(done) == len(part_ids):
            return  # khong co tin moi o partition nao

        buffers: Dict[int, List[dict]] = {p: [] for p in part_ids}
        last_offset: Dict[int, int] = {}
        total = 0

        while total < self.k.max_records_per_run and not (bounded_only and len(done) == len(part_ids)):
            msgs = self._consumer.consume(
                num_messages=min(BATCH_SIZE, self.k.max_records_per_run - total),
                timeout=self.k.poll_timeout_seconds,
            )
            if not msgs:
                break  # het ban tin moi

            for m in msgs:
                if m.error():
                    if m.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(f"[{self.cfg.name}] loi Kafka: {m.error()}")

                pid = m.partition()
                if pid in end:
                    if m.offset() >= end[pid]:
                        done.add(pid)  # vuot moc chup luc bat dau -> de lan sau
                        continue
                    if m.offset() >= end[pid] - 1:
                        done.add(pid)
                rec = {
                    "_kafka_topic": m.topic(),
                    "_kafka_partition": pid,
                    "_kafka_offset": m.offset(),
                    "_kafka_timestamp_ms": m.timestamp()[1],
                    "_kafka_key": m.key().decode("utf-8", "replace") if m.key() else None,
                    "value": m.value().decode("utf-8", "replace") if m.value() else None,
                }
                buffers[pid].append(self.stamp(rec))
                last_offset[pid] = m.offset()
                total += 1

            # Mỗi partition yield riêng một Batch, kèm offset của chính nó.
            # Runner ghi checkpoint theo từng partition nên job chết giữa
            # chừng chỉ mất tiến độ của partition chưa kịp ghi.
            for pid, buf in buffers.items():
                if len(buf) >= BATCH_SIZE:
                    yield Batch(records=buf, watermark=str(last_offset[pid]),
                                partition=str(pid))
                    buffers[pid] = []

        for pid, buf in buffers.items():
            if buf:
                yield Batch(records=buf, watermark=str(last_offset[pid]), partition=str(pid))

    def close(self) -> None:
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
