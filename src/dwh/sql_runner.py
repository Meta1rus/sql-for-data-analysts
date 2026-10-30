from __future__ import annotations

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from src.data_handlers.config import ClickhouseConfig
from src.data_handlers.spark_handler import PySparkHandler

load_dotenv()
logger = logging.getLogger(__name__)


def get_dwh_config() -> ClickhouseConfig:
    return ClickhouseConfig(
        host=os.getenv("CLICKHOUSE_DWH_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_DWH_PORT", "8124")),
        username=os.getenv("CLICKHOUSE_DWH_USER", "default"),
        password=os.getenv("CLICKHOUSE_DWH_PASSWORD", "clickhouse_dwh"),
        database=os.getenv("CLICKHOUSE_DWH_DB", "raw_layer"),
    )


def execute_sql_file(sql_file: str | Path) -> None:
    """Выполняет SQL-скрипт (разделённый ';') в ClickHouse DWH через PySparkHandler."""
    path = Path(sql_file)
    content = path.read_text(encoding="utf-8")
    dwh_config = get_dwh_config()

    with PySparkHandler() as spark_handler:
        for raw_stmt in content.split(";"):
            lines = [
                line
                for line in raw_stmt.splitlines()
                if line.strip() and not line.strip().startswith("--")
            ]
            stmt = "\n".join(lines).strip()
            if stmt:
                logger.info("Executing DWH SQL from %s: %s...", path.name, stmt[:80])
                spark_handler.execute_dwh_sql(dwh_config, stmt)
