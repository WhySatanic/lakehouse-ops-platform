from __future__ import annotations

import argparse
import json
from pathlib import Path

from lakehouse_ops.ingestion.landing import FileLandingZone
from lakehouse_ops.ingestion.models import Location
from lakehouse_ops.ingestion.open_meteo import OpenMeteoClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lakeops")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest = subparsers.add_parser("ingest-weather", help="land an Open-Meteo forecast")
    ingest.add_argument("--name", required=True)
    ingest.add_argument("--latitude", required=True, type=float)
    ingest.add_argument("--longitude", required=True, type=float)
    ingest.add_argument("--forecast-days", type=int, default=3)
    ingest.add_argument("--output", type=Path, default=Path("data/landing"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "ingest-weather":
        location = Location(args.name, args.latitude, args.longitude)
        with OpenMeteoClient() as client:
            payload = client.fetch(location, forecast_days=args.forecast_days)
        result = FileLandingZone(args.output).write(payload)
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

