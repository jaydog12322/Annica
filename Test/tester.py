# tester_full_ts.py
import os
import argparse
import pandas as pd

def read_any(path, sheet=None, encoding=None):
    ext = os.path.splitext(path)[1].lower()
    if ext in ('.xlsx', '.xls'):
        return pd.read_excel(path, sheet_name=(0 if sheet is None else sheet))
    return pd.read_csv(path, encoding=(encoding or 'cp949'))

def to_second_key(series: pd.Series) -> pd.Series:
    """
    Normalize to full second: 'YYYY-MM-DD HH:MM:SS'.
    If only HHMMSS is present, anchor to 1970-01-01.
    """
    raw = series.astype(str).str.strip()
    dt = pd.to_datetime(raw, errors="coerce", infer_datetime_format=True, dayfirst=False).dt.floor("S")
    is_nat = dt.isna()
    if is_nat.any():
        digits = raw[is_nat].str.replace(r"\D", "", regex=True).str[-6:].str.zfill(6)
        hhmmss = digits.str[0:2] + ":" + digits.str[2:4] + ":" + digits.str[4:6]
        dt_fill = pd.to_datetime("1970-01-01 " + hhmmss, errors="coerce")
        dt = dt.astype("datetime64[ns]")
        dt[is_nat] = dt_fill
    return dt.dt.strftime("%Y-%m-%d %H:%M:%S")

def clean_price(s: pd.Series) -> pd.Series:
    return pd.to_numeric(
        s.astype(str)
         .str.replace(r"[,\s]", "", regex=True)
         .str.replace(r"[^0-9.\-]", "", regex=True),
        errors="coerce"
    )

def main():
    ap = argparse.ArgumentParser(description="Check per-second min/max using full datetime key.")
    ap.add_argument("--path", required=True, help="KRX file path (CSV/XLSX)")
    ap.add_argument("--time-col", required=True, help="Timestamp column name (e.g., '일자 / 시간' or '체결시간')")
    ap.add_argument("--price-col", required=True, help="Price column name (e.g., '종가' or '현재가')")
    ap.add_argument("--sheet", default=None, help="Excel sheet name or index (optional)")
    ap.add_argument("--encoding", default=None, help="CSV encoding (e.g., cp949)")
    ap.add_argument("--second", default="13:48:08", help="HH:MM:SS to inspect")
    ap.add_argument("--date", default=None, help="YYYY-MM-DD to focus (optional)")
    args = ap.parse_args()

    df = read_any(args.path, sheet=args.sheet, encoding=args.encoding)
    ts = to_second_key(df[args.time_col])
    price = clean_price(df[args.price_col])
    sec = pd.to_datetime(ts, errors="coerce").strftime("%H:%M:%S")

    tmp = pd.DataFrame({"ts": ts, "sec": sec, "price": price}).dropna()

    # Show raw ticks for that HH:MM:SS across all dates (helps spot cross-day mixing)
    sample = tmp.loc[tmp["sec"] == args.second, ["ts", "price"]].sort_values(["ts", "price"])
    print(f"\nTicks at HH:MM:SS == {args.second}")
    print(sample.to_string(index=False) if not sample.empty else "(none)")

    # Group by FULL timestamp and show per-second min/max
    g = tmp[tmp["sec"] == args.second].groupby("ts")["price"].agg(["min", "max"]).reset_index()
    if args.date:
        g = g[g["ts"].str.startswith(args.date)]
        print(f"\nPer-second min/max for date {args.date} @ {args.second}")
    else:
        print(f"\nPer-second min/max for ALL dates @ {args.second}")

    print(g.to_string(index=False) if not g.empty else "(none)")

    if args.date:
        ts_key = f"{args.date} {args.second}"
        row = g[g["ts"] == ts_key]
        if not row.empty:
            r = row.iloc[0]
            print(f"\nCHECK: ts={ts_key} -> min={r['min']:.0f}, max={r['max']:.0f}")
        else:
            print(f"\nNo rows found for ts={ts_key}")

if __name__ == "__main__":
    main()
