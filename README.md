# robot_hardware_auto_test

## Raw SocketCAN capture

`can_temp_capture.py` records hub-motor CAN traffic without attempting to
interpret it. It writes `<prefix>_raw.csv` with each frame's receive timestamp,
CAN ID, standard/extended flag, DLC, and hexadecimal payload, plus
`<prefix>_summary.csv` with per-identifier frame counts and the last frame
payload/timestamp in each interval. Empty intervals are included in the summary.

The defaults capture `can0` for 30 minutes and write a summary every 10 seconds:

```sh
python -m pip install python-can
python can_temp_capture.py --channel can0 --duration 1800 --interval 10 \
  --output-prefix hub_motor
```

The host must have a configured SocketCAN interface and permission to access
it. Existing output files are not replaced unless `--overwrite` is supplied.
Pressing Ctrl+C stops the capture and flushes valid, usable CSV files, including
a summary row for the partial interval.

**Temperature is not decoded.** No CAN protocol specification or DBC is
available, so the payload is preserved as raw hexadecimal data; it cannot be
identified as a temperature value without that protocol information.
