#!/usr/bin/env python3
"""Remote client workload runner matching the local harness behavior."""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime


def current_timestamp() -> str:
    now = datetime.now().astimezone()
    return f"{now.strftime('%Y-%m-%dT%H:%M:%S')}.{now.microsecond // 1000:03d}{now.strftime('%z')}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a PnyxDB client workload.")
    parser.add_argument("--binary", required=True, help="Path to the pnyxdb binary.")
    parser.add_argument("--server", required=True, help="Server address host:port.")
    parser.add_argument("--rate", type=float, default=0.0, help="ADD operations per second (0 = as fast as possible).")
    parser.add_argument("--duration", type=float, default=0.0, help="Duration in seconds after initial SET.")
    parser.add_argument("--start-delay", type=float, default=2.0, help="Seconds to wait before issuing commands.")
    parser.add_argument("--key", default="counter", help="Key used for SET/ADD commands.")
    parser.add_argument("--set-value", default="1", help="Value used for the SET command.")
    parser.add_argument("--add-value", default="1", help="Value used for each ADD command.")
    return parser.parse_args()


def run_command(binary: str, server: str, args: list[str]) -> None:
    cmd = [binary, "client", "-s", server] + args
    print(f"[{current_timestamp()}] Running: {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True)


def main() -> None:
    args = parse_args()

    if args.rate < 0:
        raise SystemExit("rate must be >= 0")
    if args.duration < 0:
        raise SystemExit("duration must be >= 0")

    time.sleep(max(0.0, args.start_delay))
    run_command(args.binary, args.server, ["SET", args.key, args.set_value])

    if args.duration == 0:
        print(f"[{current_timestamp()}] Client workload finished (duration=0)", flush=True)
        return

    interval = 0.0 if args.rate == 0 else 1.0 / args.rate
    end_time = time.time() + args.duration
    while time.time() < end_time:
        run_command(args.binary, args.server, ["ADD", args.key, args.add_value])
        if interval > 0:
            time.sleep(interval)

    print(f"[{current_timestamp()}] Client workload finished", flush=True)


if __name__ == "__main__":
    main()
