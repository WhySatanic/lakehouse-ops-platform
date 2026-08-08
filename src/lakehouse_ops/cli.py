from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import boto3

from lakehouse_ops.ingestion.batch import (
    LocationManifestError,
    load_location_manifest,
    run_batch,
)
from lakehouse_ops.ingestion.landing import FileLandingZone
from lakehouse_ops.ingestion.models import Location, WeatherPayload
from lakehouse_ops.ingestion.open_meteo import OpenMeteoClient
from lakehouse_ops.ingestion.s3_landing import S3LandingZone


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
    s3_client = boto3.client(
        "s3",
        endpoint_url=args.s3_endpoint_url,
        region_name=args.s3_region,
    )
    return S3LandingZone(s3_client, bucket=args.s3_bucket, prefix=args.s3_prefix)


if __name__ == "__main__":
    raise SystemExit(main())
