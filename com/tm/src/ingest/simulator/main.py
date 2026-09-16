"""Simulator sinh dữ liệu cho 3 source (S1 payment, S2 CDC, S3 churn score).

P0: skeleton in cấu hình. Sinh dữ liệu thật ở P2 (com/tm/docs/phases.md).
"""

import argparse
import json
import sys


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="vision data simulator")
    parser.add_argument("--users", type=int, default=10_000)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--attrs", type=int, default=6)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    print(json.dumps({"component": "simulator", "status": "skeleton", **vars(args)}))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
