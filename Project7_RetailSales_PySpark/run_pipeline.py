"""Command-line entry point for the Project 7 pipeline."""

from __future__ import annotations

import argparse
import json
import logging

from config import Settings
from spark.aggregate import create_gold
from spark.commit import commit_batch_watermark
from spark.extract import extract_to_bronze
from spark.logging_utils import configure_logging
from spark.load_postgres import load_gold_to_postgres
from spark.quality import run_data_quality_checks
from spark.session import create_spark_session
from spark.transform import transform_to_silver


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Project 7 PySpark pipeline")
    parser.add_argument(
        "--check-config",
        action="store_true",
        help="validate configuration and print a secret-safe summary",
    )
    parser.add_argument(
        "--stage",
        choices=("extract", "transform", "aggregate", "load", "quality", "commit"),
        help="run one implemented pipeline stage",
    )
    parser.add_argument(
        "--batch-id",
        help="optional immutable batch identifier for the extract stage",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.check_config:
        settings = Settings.from_env()
        print(json.dumps(settings.safe_summary(), indent=2, sort_keys=True))
        return

    if args.stage not in {"extract", "transform", "aggregate", "load", "quality", "commit"}:
        raise SystemExit(
            "Choose --check-config or an implemented --stage"
        )
    if args.stage in {"transform", "quality", "commit"} and not args.batch_id:
        raise SystemExit(f"--batch-id is required for --stage {args.stage}")

    settings = Settings.from_env(require_db_password=True)
    configure_logging(settings.project_root / "logs" / "pipeline.log")
    logger = logging.getLogger("project7.pipeline")
    spark = create_spark_session(settings, f"Project7{args.stage.title()}")
    try:
        if args.stage == "extract":
            result = extract_to_bronze(spark, settings, batch_id=args.batch_id)
            logger.info(
                "Extract stage finished: batch_id=%s rows=%s output=%s",
                result.batch_id,
                result.row_count,
                result.output_path,
            )
        elif args.stage == "transform":
            result = transform_to_silver(spark, settings, batch_id=args.batch_id)
            logger.info(
                "Transform stage finished: batch_id=%s silver_rows=%s rejected=%s",
                result.batch_id,
                result.silver_count,
                result.rejected_count,
            )
        elif args.stage == "aggregate":
            result = create_gold(spark, settings)
            logger.info(
                "Aggregate stage finished: publication_id=%s datasets=%s",
                result.publication_id,
                result.dataset_counts,
            )
        elif args.stage == "load":
            result = load_gold_to_postgres(spark, settings)
            logger.info(
                "Load stage finished: load_id=%s tables=%s",
                result.load_id,
                result.table_counts,
            )
        elif args.stage == "quality":
            result = run_data_quality_checks(
                spark, settings, batch_id=args.batch_id
            )
            logger.info(
                "Quality stage finished: batch_id=%s valid=%s quarantined=%s",
                result.batch_id,
                result.valid_batch_count,
                result.quarantined_count,
            )
        else:
            result = commit_batch_watermark(
                spark, settings, batch_id=args.batch_id
            )
            logger.info(
                "Commit stage finished: batch_id=%s updated_at=%s order_id=%s",
                result.batch_id,
                result.watermark.utc_updated_at.isoformat(),
                result.watermark.order_id,
            )
    except Exception:
        logger.exception("%s stage failed", args.stage.title())
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
