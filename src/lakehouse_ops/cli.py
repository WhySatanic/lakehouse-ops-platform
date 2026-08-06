from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import boto3

from lakehouse_ops.ingestion.landing import FileLandingZone
from lakehouse_ops.ingestion.models import Location
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
    ingest.add_argument("--backend", choices=("file", "s3"), default="file")
    ingest.add_argument("--output", type=Path, default=Path("data/landing"))
    ingest.add_argument("--s3-bucket", default=os.getenv("LAKEOPS_S3_BUCKET"))
    ingest.add_argument("--s3-prefix", default=os.getenv("LAKEOPS_S3_PREFIX", "landing"))
    ingest.add_argument(
        "--s3-endpoint-url", default=os.getenv("LAKEOPS_S3_ENDPOINT_URL")
    )
    ingest.add_argument(
        "--s3-region", default=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "ingest-weather":
        location = Location(args.name, args.latitude, args.longitude)
        with OpenMeteoClient() as client:
            payload = client.fetch(location, forecast_days=args.forecast_days)
        if args.backend == "file":
            landing = FileLandingZone(args.output)
        else:
            if not args.s3_bucket:
                parser.error("--s3-bucket is required when --backend=s3")
            s3_client = boto3.client(
                "s3",
                endpoint_url=args.s3_endpoint_url,
                region_name=args.s3_region,
            )
            landing = S3LandingZone(
                s3_client, bucket=args.s3_bucket, prefix=args.s3_prefix
            )
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
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
