#!/usr/bin/env python3
import argparse, sys, pathlib, datetime as dt
from dateutil.parser import isoparse
from cds_helpers.clean_aggregate import build_series
from cds_helpers.sbsdr_fetch import probe_day

def valid_date(s: str) -> dt.date:
    return isoparse(s).date()

def _common_args(p):
    p.add_argument("--entity", required=True, help='Reference entity (e.g., "United States of America")')
    p.add_argument("--tenor-years", type=int, default=5, help="Tenor in years, e.g., 5")
    p.add_argument("--currency", default="USD", help="Currency, e.g., USD")
    p.add_argument("--start", type=valid_date, required=True, help="Start date YYYY-MM-DD")
    p.add_argument("--end", type=valid_date, required=True, help="End date YYYY-MM-DD")
    p.add_argument("--agg", default="weighted_mean",
                   choices=["weighted_mean","mean","median","last"],
                   help="Daily aggregator for multiple prints")
    p.add_argument("--out", required=True, help="Output CSV path")

def cmd_fetch(args):
    out = pathlib.Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    df = build_series(
        start=args.start, end=args.end,
        entity=args.entity, tenor_years=args.tenor_years,
        currency=args.currency, agg=args.agg
    )
    if df.empty:
        print("No data collected in the specified range (check selectors/date range/entity name).")
        sys.exit(0)
    df.to_csv(out, index=False)
    print(f"Wrote {out} with {len(df)} rows")

def cmd_probe(args):
    ok = probe_day(args.date)
    print(f"{args.date}: {'available' if ok else 'no file'}")

def main():
    # Support old "no-subcommand" call by redirecting to fetch if first token isn't a known verb
    if len(sys.argv) > 1 and sys.argv[1] not in {"fetch","probe","-h","--help"}:
        sys.argv.insert(1, "fetch")

    ap = argparse.ArgumentParser()
    sp = ap.add_subparsers(dest="cmd", required=True)

    p_fetch = sp.add_parser("fetch", help="Fetch & aggregate CDS")
    _common_args(p_fetch)
    p_fetch.set_defaults(func=cmd_fetch)

    p_probe = sp.add_parser("probe", help="Check ICE SBSDR availability for a day")
    p_probe.add_argument("--date", type=valid_date, required=True, help="YYYY-MM-DD")
    p_probe.set_defaults(func=cmd_probe)

    args = ap.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
