#!/usr/bin/env python
from __future__ import annotations
import argparse, logging, sys, pathlib, pandas as pd
from cds_helpers.clean_aggregate import build_series

LOG = logging.getLogger("SBSR")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--agg", choices=["weighted_mean","median","mean"], default="weighted_mean")
    ap.add_argument("--out", required=True, help="Output CSV path")
    args = ap.parse_args(argv)

    df = build_series(args.start, args.end, agg=args.agg)
    if df.empty:
        LOG.warning("No CDS data aggregated in the specified range.")
    outp = pathlib.Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(outp, index=False)
    LOG.info("Wrote %s rows to %s", len(df), outp)
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
