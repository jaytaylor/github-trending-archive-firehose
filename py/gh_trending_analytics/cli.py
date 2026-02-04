from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .build import build_kind
from .utils import ValidationError

VALID_KINDS = {"repository", "developer"}


def _parse_year(value: str) -> int:
    if not value.isdigit() or len(value) != 4:
        raise argparse.ArgumentTypeError(f"Invalid year: {value}")
    return int(value)


def _build_command(args: argparse.Namespace) -> int:
    kind = args.kind
    if kind not in VALID_KINDS:
        raise ValidationError(f"Unsupported kind: {kind}")
    build_kind(
        archive_root=Path(args.archive),
        analytics_root=Path(args.analytics),
        kind=kind,
        year=args.year,
        rebuild_year=args.rebuild_year,
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="gh_trending_analytics")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser("build", help="Build Parquet datasets from archive JSON")
    build_parser.add_argument("--archive", default="archive", help="Archive root directory")
    build_parser.add_argument("--analytics", default="analytics", help="Analytics output directory")
    build_parser.add_argument("--kind", required=True, choices=sorted(VALID_KINDS))
    build_parser.add_argument("--year", type=_parse_year, help="Year to build")
    build_parser.add_argument(
        "--rebuild-year",
        action="store_true",
        help="Rebuild year even if Parquet exists (default appends new dates)",
    )
    build_parser.set_defaults(func=_build_command)

    rollup_parser = subparsers.add_parser("rollup", help="Build rollup Parquet datasets")
    rollup_parser.add_argument(
        "--analytics", default="analytics", help="Analytics output directory"
    )
    rollup_parser.add_argument("--kind", required=True, choices=sorted(VALID_KINDS))
    rollup_parser.add_argument("--from-date", help="Rebuild rollups from this date (YYYY-MM-DD)")
    rollup_parser.set_defaults(func=None)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "rollup":
            from .rollup import rollup_kind

            rollup_kind(
                analytics_root=Path(args.analytics),
                kind=args.kind,
                from_date=args.from_date,
            )
            return 0
        return args.func(args)
    except ValidationError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
