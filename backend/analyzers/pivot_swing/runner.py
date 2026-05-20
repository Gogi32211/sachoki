"""CLI runner for the Pivot Swing Character Analytics Engine.

Usage:
    python -m analyzers.pivot_swing.runner \
        --csv path/to/stock_stat_tz_wlnbb_TICKER.csv \
        --out /tmp/pivot_output \
        [--pivot-left 3] [--pivot-right 3] \
        [--min-swing-pct 3.0] [--min-swing-bars 2] \
        [--ticker AAPL]

Or process all CSVs in a directory:
    python -m analyzers.pivot_swing.runner \
        --csv-dir /data/stock_stat \
        --out /tmp/pivot_output
"""
import argparse
import glob
import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Pivot Swing Character Analytics Engine")
    parser.add_argument("--csv", help="Single stock_stat_tz_wlnbb CSV file")
    parser.add_argument("--csv-dir", help="Directory containing stock_stat_tz_wlnbb_*.csv files")
    parser.add_argument("--out", required=True, help="Output directory")
    parser.add_argument("--pivot-left", type=int, default=3)
    parser.add_argument("--pivot-right", type=int, default=3)
    parser.add_argument("--min-swing-pct", type=float, default=3.0)
    parser.add_argument("--min-swing-bars", type=int, default=2)
    parser.add_argument("--ticker", default=None)
    args = parser.parse_args()

    from .pivot_analytics import run_pivot_analytics

    csv_files = []
    if args.csv:
        csv_files = [args.csv]
    elif args.csv_dir:
        csv_files = sorted(glob.glob(os.path.join(args.csv_dir, "stock_stat_tz_wlnbb_*.csv")))
    else:
        parser.print_help()
        sys.exit(1)

    if not csv_files:
        log.error("No CSV files found.")
        sys.exit(1)

    for csv_path in csv_files:
        log.info("Processing: %s", csv_path)
        out_files = run_pivot_analytics(
            csv_path=csv_path,
            output_dir=args.out,
            pivot_left=args.pivot_left,
            pivot_right=args.pivot_right,
            min_swing_return_pct=args.min_swing_pct,
            min_swing_bars=args.min_swing_bars,
            ticker=args.ticker,
        )
        log.info("Wrote %d files:", len(out_files))
        for name, path in sorted(out_files.items()):
            log.info("  %s", path)


if __name__ == "__main__":
    main()
