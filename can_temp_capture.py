#!/usr/bin/env python3
"""Capture raw SocketCAN frames and periodic per-identifier summaries.

This script does not decode temperature. A CAN protocol specification or DBC
file is required to interpret payload bytes as sensor values.
"""

import argparse
import csv
from datetime import datetime, timedelta
import math
from pathlib import Path
import signal
import sys
import time


DEFAULT_DURATION = 30 * 60
DEFAULT_INTERVAL = 10
RECV_POLL_SECONDS = 1

RAW_FIELDS = ("timestamp", "can_id", "is_extended_id", "dlc", "data")
SUMMARY_FIELDS = (
    "interval_start",
    "interval_end",
    "can_id",
    "is_extended_id",
    "frame_count",
    "last_timestamp",
    "last_dlc",
    "last_data",
)


class CaptureError(Exception):
    """An expected configuration, CAN, or output-file error."""


def positive_seconds(value):
    try:
        seconds = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number of seconds") from exc
    if not math.isfinite(seconds) or seconds <= 0:
        raise argparse.ArgumentTypeError("must be a finite number greater than zero")
    return seconds


def build_parser():
    parser = argparse.ArgumentParser(
        description=(
            "Capture raw CAN frames from a SocketCAN interface and write a "
            "per-interval frame-count summary. Payloads are not decoded; "
            "temperature cannot be determined without the CAN protocol/DBC."
        ),
        epilog=(
            "Example: python can_temp_capture.py --channel can0 "
            "--duration 1800 --interval 10 --output-prefix hub_motor"
        ),
    )
    parser.add_argument(
        "--channel", default="can0", help="SocketCAN channel (default: can0)"
    )
    parser.add_argument(
        "--duration",
        type=positive_seconds,
        default=DEFAULT_DURATION,
        metavar="SECONDS",
        help="capture duration in seconds (default: 1800)",
    )
    parser.add_argument(
        "--interval",
        type=positive_seconds,
        default=DEFAULT_INTERVAL,
        metavar="SECONDS",
        help="summary interval in seconds (default: 10)",
    )
    parser.add_argument(
        "--output-prefix",
        default="can_capture",
        metavar="PREFIX",
        help="output path prefix; writes PREFIX_raw.csv and PREFIX_summary.csv",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="replace existing output files",
    )
    return parser


def _format_can_id(message):
    return "0x{:X}".format(message.arbitration_id)


def _write_summary(writer, start_wall, interval_index, interval_seconds, duration,
                   frames, end_elapsed):
    interval_start_elapsed = interval_index * interval_seconds
    interval_end_elapsed = min(
        (interval_index + 1) * interval_seconds, duration, end_elapsed
    )
    interval_start = start_wall + timedelta(seconds=interval_start_elapsed)
    interval_end = start_wall + timedelta(seconds=interval_end_elapsed)
    common = {
        "interval_start": interval_start.isoformat(timespec="microseconds"),
        "interval_end": interval_end.isoformat(timespec="microseconds"),
    }
    if not frames:
        writer.writerow(
            {
                **common,
                "can_id": "",
                "is_extended_id": "",
                "frame_count": 0,
                "last_timestamp": "",
                "last_dlc": "",
                "last_data": "",
            }
        )
        return

    for (is_extended_id, can_id), stats in sorted(frames.items()):
        writer.writerow(
            {
                **common,
                "can_id": "0x{:X}".format(can_id),
                "is_extended_id": str(is_extended_id).lower(),
                "frame_count": stats["count"],
                "last_timestamp": stats["last_timestamp"],
                "last_dlc": stats["last_dlc"],
                "last_data": stats["last_data"],
            }
        )


def _record_frame(writer, message, timestamp):
    data = bytes(message.data).hex().upper()
    writer.writerow(
        {
            "timestamp": timestamp,
            "can_id": _format_can_id(message),
            "is_extended_id": str(bool(message.is_extended_id)).lower(),
            "dlc": message.dlc,
            "data": data,
        }
    )
    return data


def capture(bus, can_error_type, raw_writer, summary_writer, raw_file, summary_file, args):
    """Read frames until duration expires or SIGINT requests a clean stop."""
    start_wall = datetime.now().astimezone()
    start_mono = time.monotonic()
    interval_count = math.ceil(args.duration / args.interval)
    interval_index = 0
    frames = {}
    stop_requested = False
    interrupted = False

    def request_stop(_signum, _frame):
        nonlocal stop_requested
        stop_requested = True

    previous_sigint_handler = signal.signal(signal.SIGINT, request_stop)

    def finish_due_intervals(elapsed):
        nonlocal interval_index, frames
        while interval_index < interval_count:
            interval_end = min(
                (interval_index + 1) * args.interval, args.duration
            )
            if elapsed < interval_end:
                break
            _write_summary(
                summary_writer,
                start_wall,
                interval_index,
                args.interval,
                args.duration,
                frames,
                interval_end,
            )
            summary_file.flush()
            interval_index += 1
            frames = {}

    try:
        while True:
            elapsed = time.monotonic() - start_mono
            finish_due_intervals(elapsed)

            if stop_requested:
                interrupted = True
                if interval_index < interval_count:
                    _write_summary(
                        summary_writer,
                        start_wall,
                        interval_index,
                        args.interval,
                        args.duration,
                        frames,
                        elapsed,
                    )
                    summary_file.flush()
                break

            if elapsed >= args.duration:
                break

            current_interval_end = min(
                (interval_index + 1) * args.interval, args.duration
            )
            timeout = min(
                current_interval_end - elapsed,
                args.duration - elapsed,
                RECV_POLL_SECONDS,
            )
            try:
                message = bus.recv(timeout=max(0, timeout))
            except (can_error_type, OSError) as exc:
                raise CaptureError("CAN receive failed: {}".format(exc)) from exc

            if message is None:
                continue

            received_elapsed = time.monotonic() - start_mono
            finish_due_intervals(received_elapsed)
            if received_elapsed >= args.duration:
                continue

            timestamp = datetime.now().astimezone().isoformat(
                timespec="microseconds"
            )
            data = _record_frame(raw_writer, message, timestamp)
            raw_file.flush()

            key = (bool(message.is_extended_id), message.arbitration_id)
            stats = frames.setdefault(
                key,
                {
                    "count": 0,
                    "last_timestamp": "",
                    "last_dlc": "",
                    "last_data": "",
                },
            )
            stats["count"] += 1
            stats["last_timestamp"] = timestamp
            stats["last_dlc"] = message.dlc
            stats["last_data"] = data
            summary_file.flush()
    finally:
        signal.signal(signal.SIGINT, previous_sigint_handler)

    return interrupted


def run(args):
    raw_path = Path(args.output_prefix + "_raw.csv")
    summary_path = Path(args.output_prefix + "_summary.csv")
    if raw_path.resolve() == summary_path.resolve():
        raise CaptureError("raw and summary output paths must be different")
    if not args.overwrite:
        existing = [str(path) for path in (raw_path, summary_path) if path.exists()]
        if existing:
            raise CaptureError(
                "output already exists (use --overwrite to replace): {}".format(
                    ", ".join(existing)
                )
            )

    try:
        import can
    except ImportError as exc:
        raise CaptureError(
            "python-can is required; install it with 'python -m pip install python-can'"
        ) from exc

    try:
        bus = can.Bus(interface="socketcan", channel=args.channel)
    except (can.CanError, OSError, ValueError) as exc:
        raise CaptureError(
            "could not initialize SocketCAN channel {!r}: {}".format(
                args.channel, exc
            )
        ) from exc

    try:
        with raw_path.open("w", newline="", encoding="utf-8") as raw_file, summary_path.open(
            "w", newline="", encoding="utf-8"
        ) as summary_file:
            raw_writer = csv.DictWriter(raw_file, fieldnames=RAW_FIELDS)
            summary_writer = csv.DictWriter(summary_file, fieldnames=SUMMARY_FIELDS)
            raw_writer.writeheader()
            summary_writer.writeheader()
            raw_file.flush()
            summary_file.flush()
            return capture(
                bus,
                can.CanError,
                raw_writer,
                summary_writer,
                raw_file,
                summary_file,
                args,
            )
    except OSError as exc:
        raise CaptureError("could not write capture files: {}".format(exc)) from exc
    finally:
        try:
            bus.shutdown()
        except (can.CanError, OSError) as exc:
            print(
                "warning: error while shutting down CAN bus: {}".format(exc),
                file=sys.stderr,
            )


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.channel.strip():
        parser.error("--channel must not be empty")
    if not args.output_prefix.strip():
        parser.error("--output-prefix must not be empty")

    try:
        interrupted = run(args)
    except CaptureError as exc:
        print("error: {}".format(exc), file=sys.stderr)
        return 1

    if interrupted:
        print("Capture interrupted; flushed partial CSV files.", file=sys.stderr)
        return 130
    print("Capture complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
