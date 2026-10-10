from ..config import SilverConfig, WriterType
from .base import Writer


def build_writer(spark, cfg: SilverConfig, source_label: str) -> Writer:
    if cfg.writer.type is WriterType.SCD2:
        from .scd2 import Scd2Writer
        return Scd2Writer(spark, cfg, source_label)
    if cfg.writer.type is WriterType.SCD4A:
        from .scd4a import Scd4aWriter
        return Scd4aWriter(spark, cfg, source_label)
    from .simple import SimpleWriter
    return SimpleWriter(spark, cfg, source_label)


__all__ = ["build_writer", "Writer"]
