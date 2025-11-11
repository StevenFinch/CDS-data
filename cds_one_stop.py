# cds_one_stop.py
from __future__ import annotations
import argparse, sys, logging, pathlib
import pandas as pd
from cds_helpers.clean_aggregate import build_series

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("CDS")

def cmd_probe(args: argparse.Namespace) -> None:
    print("OK: cds_one_stop is installed and importable.")

def cmd_fetch(args: argparse.Namespace) -> None:
    out = pathlib.Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df = build_series(
        start=args.start, end=args.end,
        entity=args.entity, currency=args.currency,
        tenor_years=args.tenor_years, agg=args.agg
    )
    if df.empty:
        LOG.warning("No rows aggregated between %s and %s for '%s'.", args.start, args.end, args.entity)
        # still write a header-only CSV for determinism
        df = pd.DataFrame(columns=["date", "value_bps", "count", "weight_sum"])
    df.to_csv(out, index=False)
    LOG.info("Wrote %s rows to %s", len(df), out)

def main(argv=None):
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    s1 = sub.add_parser("probe")
    s1.set_defaults(func=cmd_probe)

    s2 = sub.add_parser("fetch")
    s2.add_argument("--entity", required=True, help='e.g., "United States of America"')
    s2.add_argument("--tenor-years", type=int, default=5)
    s2.add_argument("--currency", default="USD")
    s2.add_argument("--start", required=True)
    s2.add_argument("--end", required=True)
    s2.add_argument("--agg", default="weighted_mean", choices=["weighted_mean","median","mean"])
    s2.add_argument("--out", required=True)
    s2.set_defaults(func=cmd_fetch)

    args = p.parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main(sys.argv[1:])
