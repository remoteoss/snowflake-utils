import argparse
from pathlib import Path

from ._convert import convert_dir
from ._scan import scan_dir


def _report(directory: Path) -> int:
    findings = scan_dir(directory)
    for finding in findings:
        print(finding)
    return 1 if findings else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m snowflake_utils.testing.vcr")
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check", help="Fail if a cassette holds secrets.")
    check.add_argument("directory", type=Path)
    convert = commands.add_parser(
        "convert", help="Rewrite snowflake-vcrpy cassettes in place, then check them."
    )
    convert.add_argument("directory", type=Path)
    args = parser.parse_args(argv)
    if args.command == "convert":
        print(f"converted {convert_dir(args.directory)} cassettes")
    return _report(args.directory)
