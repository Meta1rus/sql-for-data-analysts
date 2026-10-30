import argparse
import logging

from src.data_handlers.spark_handler import PySparkHandler
from src.dwh.raw_layer_schemas import (
    RAW_LAYER_TABLES,
    init_dwh_schemas_and_raw_tables,
)
from src.dwh.raw_loader import RawLayerLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Синхронизация таблиц источников в raw_layer ClickHouse DWH через PySpark"
    )
    parser.add_argument(
        "--table",
        type=str,
        default=None,
        choices=list(RAW_LAYER_TABLES.keys()),
        help="Имя конкретной таблицы для синхронизации (по умолчанию все 13 таблиц)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Принудительная полная перезаливка (TRUNCATE + INSERT)",
    )
    args = parser.parse_args()

    with PySparkHandler() as spark_handler:
        loader = RawLayerLoader(spark_handler)
        init_dwh_schemas_and_raw_tables(spark_handler, loader.ch_dwh_config)

        if args.table:
            spec = RAW_LAYER_TABLES[args.table]
            if args.full or spec["mode"] == "truncate":
                loader.sync_truncate_table(args.table, spec["source"])
            else:
                loader.sync_incremental_table(
                    args.table, spec["source"], spec["watermark_col"]
                )
        else:
            loader.sync_all_raw_tables(force_full=args.full)


if __name__ == "__main__":
    main()
