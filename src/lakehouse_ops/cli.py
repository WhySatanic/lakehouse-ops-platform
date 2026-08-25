from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import boto3

from lakehouse_ops.doctor import DoctorReport, check_file_landing, check_s3_bucket
from lakehouse_ops.iceberg.metadata import IcebergMetadataCollector
from lakehouse_ops.iceberg.planner import IcebergMaintenancePlanner, MaintenancePolicy
from lakehouse_ops.ingestion.audit import audit_file_landing
from lakehouse_ops.ingestion.batch import (
    LocationManifestError,
    load_location_manifest,
    run_batch,
)
from lakehouse_ops.ingestion.landing import FileLandingZone
from lakehouse_ops.ingestion.models import Location, WeatherPayload
from lakehouse_ops.ingestion.open_meteo import OpenMeteoClient
from lakehouse_ops.ingestion.s3_landing import S3LandingZone
from lakehouse_ops.trino import TrinoClient
from lakehouse_ops.trino_baseline import (
    QueryCorpusError,
    capture_baseline,
    load_query_corpus,
)
from lakehouse_ops.trino_experiment import (
    TrinoExperimentError,
    capture_compaction_phase,
    compare_compaction_phases,
)
from lakehouse_ops.trino_partition_experiment import (
    PartitionExperimentError,
    capture_partition_pruning_experiment,
)
from lakehouse_ops.trino_sort_experiment import (
    SortExperimentError,
    capture_sort_order_experiment,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lakeops")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest = subparsers.add_parser("ingest-weather", help="land an Open-Meteo forecast")
    ingest.add_argument("--name", required=True)
    ingest.add_argument("--latitude", required=True, type=float)
    ingest.add_argument("--longitude", required=True, type=float)
    ingest.add_argument("--forecast-days", type=int, default=3)
    _add_landing_arguments(ingest)

    batch = subparsers.add_parser(
        "ingest-weather-batch", help="land forecasts from a location manifest"
    )
    batch.add_argument("--locations", required=True, type=Path)
    batch.add_argument("--forecast-days", type=int, default=3)
    batch.add_argument("--max-workers", type=int, default=4)
    _add_landing_arguments(batch)

    doctor = subparsers.add_parser("doctor", help="check landing backend readiness")
    _add_landing_arguments(doctor)

    audit = subparsers.add_parser("audit-landing", help="verify landed file integrity")
    audit.add_argument("--output", type=Path, default=Path("data/landing"))

    metadata = subparsers.add_parser(
        "collect-iceberg-metadata", help="collect an Iceberg table health snapshot"
    )
    metadata.add_argument(
        "--server", default=os.getenv("TRINO_SERVER", "http://localhost:8080")
    )
    metadata.add_argument("--user", default="lakehouse-ops")
    metadata.add_argument("--catalog", default="lakehouse")
    metadata.add_argument("--schema", default="silver")
    metadata.add_argument("--table", default="weather_hourly")

    baseline = subparsers.add_parser(
        "capture-trino-baseline", help="run a versioned query corpus with Trino metrics"
    )
    baseline.add_argument("--corpus", required=True, type=Path)
    baseline.add_argument(
        "--server", default=os.getenv("TRINO_SERVER", "http://localhost:8080")
    )
    baseline.add_argument("--user", default="lakehouse-performance")

    compaction_capture = subparsers.add_parser(
        "capture-trino-compaction",
        help="capture one phase of the Iceberg compaction experiment",
    )
    compaction_capture.add_argument(
        "--server", default=os.getenv("TRINO_SERVER", "http://localhost:8080")
    )
    compaction_capture.add_argument("--user", default="lakehouse-performance")
    compaction_capture.add_argument("--catalog", default="lakehouse")
    compaction_capture.add_argument("--schema", default="ops")
    compaction_capture.add_argument("--table", default="maintenance_fixture")
    compaction_capture.add_argument("--phase", required=True, choices=("before", "after"))
    compaction_capture.add_argument("--repetitions", type=int, default=3)

    compaction_compare = subparsers.add_parser(
        "compare-trino-compaction",
        help="compare compaction phases with the applied maintenance report",
    )
    compaction_compare.add_argument("--before", required=True, type=Path)
    compaction_compare.add_argument("--after", required=True, type=Path)
    compaction_compare.add_argument("--execution", required=True, type=Path)

    partition_pruning = subparsers.add_parser(
        "capture-trino-partition-pruning",
        help="compare identical unpartitioned and day-partitioned Iceberg tables",
    )
    partition_pruning.add_argument(
        "--server", default=os.getenv("TRINO_SERVER", "http://localhost:8080")
    )
    partition_pruning.add_argument("--user", default="lakehouse-performance")
    partition_pruning.add_argument("--catalog", default="lakehouse")
    partition_pruning.add_argument("--schema", default="ops")
    partition_pruning.add_argument(
        "--unpartitioned-table", default="pruning_unpartitioned"
    )
    partition_pruning.add_argument("--partitioned-table", default="pruning_partitioned")
    partition_pruning.add_argument("--target-day", default="2026-01-16")
    partition_pruning.add_argument("--repetitions", type=int, default=3)

    sort_order = subparsers.add_parser(
        "capture-trino-sort-order",
        help="compare identical unpartitioned Iceberg tables with different file ordering",
    )
    sort_order.add_argument(
        "--server", default=os.getenv("TRINO_SERVER", "http://localhost:8080")
    )
    sort_order.add_argument("--user", default="lakehouse-performance")
    sort_order.add_argument("--catalog", default="lakehouse")
    sort_order.add_argument("--schema", default="ops")
    sort_order.add_argument("--baseline-table", default="sort_baseline")
    sort_order.add_argument("--sorted-table", default="sort_ordered")
    sort_order.add_argument("--range-start", type=int, default=30_000)
    sort_order.add_argument("--range-size", type=int, default=128)
    sort_order.add_argument("--repetitions", type=int, default=3)

    plan = subparsers.add_parser(
        "plan-iceberg-maintenance", help="create an explainable Iceberg maintenance plan"
    )
    plan.add_argument("--input", required=True, type=Path)
    plan.add_argument("--target-file-size-bytes", type=int, default=128 * 1024 * 1024)
    plan.add_argument("--small-file-ratio", type=float, default=0.5)
    plan.add_argument("--min-data-files", type=int, default=4)
    plan.add_argument("--min-manifest-count", type=int, default=8)
    plan.add_argument("--max-manifests-per-data-file", type=float, default=2.0)
    plan.add_argument("--snapshot-retention-hours", type=int, default=168)
    plan.add_argument("--min-snapshots-to-keep", type=int, default=3)
    plan.add_argument("--max-snapshots-to-expire", type=int, default=50)
    plan.add_argument("--enable-orphan-inspection", action="store_true")
    plan.add_argument("--orphan-retention-hours", type=int, default=168)
    plan.add_argument("--max-orphan-files", type=int, default=1000)
    return parser


def _add_landing_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--backend", choices=("file", "s3"), default="file")
    parser.add_argument("--output", type=Path, default=Path("data/landing"))
    parser.add_argument("--s3-bucket", default=os.getenv("LAKEOPS_S3_BUCKET"))
    parser.add_argument("--s3-prefix", default=os.getenv("LAKEOPS_S3_PREFIX", "landing"))
    parser.add_argument("--s3-endpoint-url", default=os.getenv("LAKEOPS_S3_ENDPOINT_URL"))
    parser.add_argument("--s3-region", default=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "ingest-weather":
        location = Location(args.name, args.latitude, args.longitude)
        landing = _create_landing(args, parser)
        payload = _fetch_weather(location, args.forecast_days)
        result = landing.write(payload)
        print(
            json.dumps(
                {
                    "path": str(result.path),
                    "checksum": result.checksum,
                    "created": result.created,
                },
                sort_keys=True,
            )
        )
        return 0
    if args.command == "ingest-weather-batch":
        if not 1 <= args.forecast_days <= 16:
            parser.error("--forecast-days must be between 1 and 16")
        if not 1 <= args.max_workers <= 16:
            parser.error("--max-workers must be between 1 and 16")
        try:
            locations = load_location_manifest(args.locations)
        except LocationManifestError as error:
            parser.error(str(error))
        landing = _create_landing(args, parser)
        report = run_batch(
            locations,
            lambda location: landing.write(_fetch_weather(location, args.forecast_days)),
            max_workers=args.max_workers,
        )
        print(json.dumps(report.as_dict(), sort_keys=True))
        return 1 if report.failed else 0
    if args.command == "doctor":
        if args.backend == "file":
            check = check_file_landing(args.output)
        else:
            if not args.s3_bucket:
                parser.error("--s3-bucket is required when --backend=s3")
            check = check_s3_bucket(_create_s3_client(args), args.s3_bucket)
        report = DoctorReport((check,))
        print(json.dumps(report.as_dict(), sort_keys=True))
        return 0 if report.healthy else 1
    if args.command == "audit-landing":
        report = audit_file_landing(args.output)
        print(json.dumps(report.as_dict(), sort_keys=True))
        return 0 if report.healthy else 1
    if args.command == "collect-iceberg-metadata":
        with TrinoClient(args.server, user=args.user) as client:
            report = IcebergMetadataCollector(client).collect(
                args.catalog, args.schema, args.table
            )
        print(json.dumps(report.as_dict(), sort_keys=True))
        return 0
    if args.command == "capture-trino-baseline":
        try:
            corpus = load_query_corpus(args.corpus)
            with TrinoClient(args.server, user=args.user) as client:
                report = capture_baseline(client, corpus)
        except QueryCorpusError as error:
            parser.error(str(error))
        print(json.dumps(report, sort_keys=True))
        return 0
    if args.command == "capture-trino-compaction":
        try:
            with TrinoClient(args.server, user=args.user) as client:
                report = capture_compaction_phase(
                    client,
                    catalog=args.catalog,
                    schema=args.schema,
                    table=args.table,
                    phase=args.phase,
                    repetitions=args.repetitions,
                )
        except TrinoExperimentError as error:
            parser.error(str(error))
        print(json.dumps(report, sort_keys=True))
        return 0
    if args.command == "compare-trino-compaction":
        try:
            before = json.loads(args.before.read_text(encoding="utf-8"))
            after = json.loads(args.after.read_text(encoding="utf-8"))
            execution = json.loads(args.execution.read_text(encoding="utf-8"))
            report = compare_compaction_phases(before, after, execution)
        except (OSError, json.JSONDecodeError, TrinoExperimentError) as error:
            parser.error(str(error))
        print(json.dumps(report, sort_keys=True))
        return 0
    if args.command == "capture-trino-partition-pruning":
        try:
            with TrinoClient(args.server, user=args.user) as client:
                report = capture_partition_pruning_experiment(
                    client,
                    catalog=args.catalog,
                    schema=args.schema,
                    unpartitioned_table=args.unpartitioned_table,
                    partitioned_table=args.partitioned_table,
                    target_day=args.target_day,
                    repetitions=args.repetitions,
                )
        except PartitionExperimentError as error:
            parser.error(str(error))
        print(json.dumps(report, sort_keys=True))
        return 0
    if args.command == "capture-trino-sort-order":
        try:
            with TrinoClient(args.server, user=args.user) as client:
                report = capture_sort_order_experiment(
                    client,
                    catalog=args.catalog,
                    schema=args.schema,
                    baseline_table=args.baseline_table,
                    sorted_table=args.sorted_table,
                    range_start=args.range_start,
                    range_size=args.range_size,
                    repetitions=args.repetitions,
                )
        except SortExperimentError as error:
            parser.error(str(error))
        print(json.dumps(report, sort_keys=True))
        return 0
    if args.command == "plan-iceberg-maintenance":
        try:
            report = json.loads(args.input.read_text(encoding="utf-8"))
            policy = MaintenancePolicy(
                target_file_size_bytes=args.target_file_size_bytes,
                small_file_ratio=args.small_file_ratio,
                min_data_files=args.min_data_files,
                min_manifest_count=args.min_manifest_count,
                max_manifests_per_data_file=args.max_manifests_per_data_file,
                snapshot_retention_hours=args.snapshot_retention_hours,
                min_snapshots_to_keep=args.min_snapshots_to_keep,
                max_snapshots_to_expire=args.max_snapshots_to_expire,
                orphan_inspection_enabled=args.enable_orphan_inspection,
                orphan_retention_hours=args.orphan_retention_hours,
                max_orphan_files=args.max_orphan_files,
            )
            plan = IcebergMaintenancePlanner(policy).plan(report)
        except (OSError, json.JSONDecodeError, ValueError) as error:
            parser.error(str(error))
        print(json.dumps(plan.as_dict(), sort_keys=True))
        return 0
    return 2


def _fetch_weather(location: Location, forecast_days: int) -> WeatherPayload:
    with OpenMeteoClient() as client:
        return client.fetch(location, forecast_days=forecast_days)


def _create_landing(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> FileLandingZone | S3LandingZone:
    if args.backend == "file":
        return FileLandingZone(args.output)
    if not args.s3_bucket:
        parser.error("--s3-bucket is required when --backend=s3")
    s3_client = _create_s3_client(args)
    return S3LandingZone(s3_client, bucket=args.s3_bucket, prefix=args.s3_prefix)


def _create_s3_client(args: argparse.Namespace) -> Any:
    return boto3.client(
        "s3",
        endpoint_url=args.s3_endpoint_url,
        region_name=args.s3_region,
    )


if __name__ == "__main__":
    raise SystemExit(main())
