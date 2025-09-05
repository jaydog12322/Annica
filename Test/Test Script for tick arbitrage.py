#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
compare_spreads_gui_tk.py
-------------------------
A dependency-light GUI (Tkinter) to compute per-second cross-exchange spreads
between two tick files (KRX vs NXT). Works on 32-bit Python.

- Supports CSV and Excel (.xlsx/.xls)
- Lets you browse for files, set optional column names/sheets, choose CSV encoding
- Writes an output CSV and shows a preview (first 1,000 rows)

Run:
    python compare_spreads_gui_tk.py

Requires:
    pip install numpy pandas openpyxl   # openpyxl only if you load .xlsx

Author: ChatGPT
"""
from __future__ import annotations

import os
import sys
from typing import Optional, Dict

import numpy as np
import pandas as pd

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# -----------------------------
# Data processing utilities
# -----------------------------
PRICE_CANDIDATES = [
    "price", "Price", "PRICE",
    "현재가", "체결가", "거래가격", "체결가격", "종가"
]
TIME_CANDIDATES = [
    "timestamp", "time", "Timestamp", "Time", "DATE", "DATETIME",
    "체결시간", "시간", "시각"
]


def _try_read_csv(path: str, chunksize: int, encoding: Optional[str] = None):
    """Yield pandas chunks from a CSV using a best-effort encoding fallback."""
    tried = []
    encodings = [encoding] if encoding else ["utf-8-sig", "cp949", "euc-kr"]
    last_err = None
    for enc in encodings:
        try:
            return pd.read_csv(path, encoding=enc, chunksize=chunksize, low_memory=False)
        except Exception as e:
            tried.append(enc)
            last_err = e
    raise RuntimeError(
        f"Failed to read CSV '{path}' with encodings {tried}. Last error: {last_err}"
    )


def _excel_iter(path: str, sheet: Optional[str | int] = None):
    """Yield a single DataFrame from an Excel sheet (no chunking)."""
    if sheet is None:
        sheet = 0
    try:
        df = pd.read_excel(path, sheet_name=sheet)
    except Exception as e:
        raise RuntimeError(
            f"Failed reading Excel '{path}' (sheet={sheet}). If .xlsx, install 'openpyxl'.\n{e}"
        )
    if isinstance(df, dict):
        df = next(iter(df.values()))
    yield df


def _iter_input(path: str, chunksize: int, encoding: Optional[str], sheet: Optional[str | int]):
    ext = os.path.splitext(path)[1].lower()
    if ext in {".csv", ".txt"}:
        return _try_read_csv(path, chunksize=chunksize, encoding=encoding)
    elif ext in {".xlsx", ".xls"}:
        return _excel_iter(path, sheet)
    else:
        return _try_read_csv(path, chunksize=chunksize, encoding=encoding)


def infer_timestamp_column(df: pd.DataFrame, explicit_time_col: Optional[str]) -> str:
    if explicit_time_col:
        if explicit_time_col not in df.columns:
            raise ValueError(f"Timestamp column '{explicit_time_col}' not found. Columns: {list(df.columns)}")
        return explicit_time_col
    for c in TIME_CANDIDATES:
        if c in df.columns:
            return c
    return df.columns[0]


def infer_price_column(df: pd.DataFrame, explicit_price_col: Optional[str], ts_col: str) -> str:
    if explicit_price_col:
        if explicit_price_col not in df.columns:
            raise ValueError(f"Price column '{explicit_price_col}' not found. Columns: {list(df.columns)}")
        return explicit_price_col
    for c in PRICE_CANDIDATES:
        if c in df.columns and c != ts_col:
            return c
    numeric_counts: Dict[str, int] = {}
    for c in df.columns:
        if c == ts_col:
            continue
        s = pd.to_numeric(df[c], errors="coerce")
        count = int(s.notna().sum())
        if count > 0:
            numeric_counts[c] = count
    if not numeric_counts:
        raise ValueError("Could not infer a numeric price column. Specify it in the GUI.")
    return max(numeric_counts, key=numeric_counts.get)


def to_second_key(series: pd.Series) -> pd.Series:
    """
    Normalize to a full second-level timestamp key: 'YYYY-MM-DD HH:MM:SS'.
    If only HHMMSS is present, anchor to a dummy date so both files align.
    """
    raw = series.astype(str).str.strip()
    dt = pd.to_datetime(raw, errors="coerce", infer_datetime_format=True, dayfirst=False)
    dt = dt.dt.floor("S")  # snap to whole second

    is_nat = dt.isna()
    if is_nat.any():
        # fallback for HHMMSS-only inputs
        digits = raw[is_nat].str.replace(r"\D", "", regex=True).str[-6:].str.zfill(6)
        hhmmss = digits.str.slice(0, 2) + ":" + digits.str.slice(2, 4) + ":" + digits.str.slice(4, 6)
        dt_fill = pd.to_datetime("1970-01-01 " + hhmmss, errors="coerce")
        dt = dt.astype("datetime64[ns]")
        dt[is_nat] = dt_fill

    return dt.dt.strftime("%Y-%m-%d %H:%M:%S")



def aggregate_to_second(
    path: str,
    time_col: Optional[str] = None,
    price_col: Optional[str] = None,
    encoding: Optional[str] = None,
    chunksize: int = 500_000,
    sheet: Optional[str | int] = None,
) -> pd.DataFrame:
    """
    Read a tick file (CSV chunked, Excel whole) and return a compact per-second table with min/max price.
    Aggregates by FULL timestamp key 'YYYY-MM-DD HH:MM:SS' so different days don't mix.
    Returns: ['ts', 'min_price', 'max_price']
    """
    reader = _iter_input(path, chunksize=chunksize, encoding=encoding, sheet=sheet)

    agg_min: Dict[str, float] = {}
    agg_max: Dict[str, float] = {}

    ts_name: Optional[str] = None
    price_name: Optional[str] = None

    for _, chunk in enumerate(reader):
        if ts_name is None:
            ts_name = infer_timestamp_column(chunk, time_col)
            price_name = infer_price_column(chunk, price_col, ts_name)

        # Build a (ts, price) two-column frame; clean price (commas/whitespace/odd chars)
        ts = to_second_key(chunk[ts_name])  # 'YYYY-MM-DD HH:MM:SS'
        raw_price = (chunk[price_name].astype(str)
                     .str.replace(r'[,\s]', '', regex=True)
                     .str.replace(r'[^0-9.\-]', '', regex=True))
        price = pd.to_numeric(raw_price, errors="coerce")

        df = pd.DataFrame({"ts": ts, "price": price}).dropna()
        if df.empty:
            continue

        grouped = df.groupby("ts")["price"].agg(["min", "max"]).reset_index()

        # Update global aggregators
        for _, row in grouped.iterrows():
            key = row["ts"]
            vmin = float(row["min"])
            vmax = float(row["max"])
            if key in agg_min:
                if vmin < agg_min[key]:
                    agg_min[key] = vmin
            else:
                agg_min[key] = vmin
            if key in agg_max:
                if vmax > agg_max[key]:
                    agg_max[key] = vmax
            else:
                agg_max[key] = vmax

    if not agg_min:
        raise RuntimeError(f"No valid price data found in '{path}'. Check columns/encoding/sheet.")

    res = (
        pd.DataFrame({"ts": list(agg_min.keys())})
        .assign(min_price=lambda d: d["ts"].map(agg_min))
        .assign(max_price=lambda d: d["ts"].map(agg_max))
        .sort_values("ts")
        .reset_index(drop=True)
    )
    return res



def compute_cross_exchange_spreads(
    krx_path: str,
    nxt_path: str,
    time_col: Optional[str] = None,
    price_col: Optional[str] = None,
    encoding: Optional[str] = None,
    chunksize: int = 500_000,
    krx_sheet: Optional[str | int] = None,
    nxt_sheet: Optional[str | int] = None,
) -> pd.DataFrame:
    """
    Combine two per-second aggregations and compute cross-exchange spreads.
    Joins on FULL timestamp 'ts' (YYYY-MM-DD HH:MM:SS) to avoid mixing days.
    Returns columns:
      ts, sec, krx_min, krx_max, nxt_min, nxt_max,
      krx_over_nxt_pct, nxt_over_krx_pct, best_direction, best_spread_pct
    """
    krx = aggregate_to_second(krx_path, time_col, price_col, encoding, chunksize, sheet=krx_sheet).rename(
        columns={"min_price": "krx_min", "max_price": "krx_max"}
    )
    nxt = aggregate_to_second(nxt_path, time_col, price_col, encoding, chunksize, sheet=nxt_sheet).rename(
        columns={"min_price": "nxt_min", "max_price": "nxt_max"}
    )

    # Align by full-second timestamp present in both files
    merged = pd.merge(krx, nxt, on="ts", how="inner")

    # For readability, also include pure time-of-day HH:MM:SS
    sec_series = pd.to_datetime(merged["ts"], errors="coerce").dt.strftime("%H:%M:%S")

    # Avoid division by zero
    nxt_min_pos = merged["nxt_min"] > 0
    krx_min_pos = merged["krx_min"] > 0

    # Directed spreads
    krx_over_nxt = pd.Series(np.nan, index=merged.index, dtype="float64")
    nxt_over_krx = pd.Series(np.nan, index=merged.index, dtype="float64")

    # (krx_max - nxt_min) / nxt_min * 100
    valid_kn = nxt_min_pos
    krx_over_nxt.loc[valid_kn] = (
        (merged.loc[valid_kn, "krx_max"] - merged.loc[valid_kn, "nxt_min"]) /
        merged.loc[valid_kn, "nxt_min"] * 100.0
    )

    # (nxt_max - krx_min) / krx_min * 100
    valid_nk = krx_min_pos
    nxt_over_krx.loc[valid_nk] = (
        (merged.loc[valid_nk, "nxt_max"] - merged.loc[valid_nk, "krx_min"]) /
        merged.loc[valid_nk, "krx_min"] * 100.0
    )

    # We only care about positive spreads
    krx_over_nxt = krx_over_nxt.clip(lower=0)
    nxt_over_krx = nxt_over_krx.clip(lower=0)

    best_spread = pd.concat([
        krx_over_nxt.rename("krx_over_nxt_pct"),
        nxt_over_krx.rename("nxt_over_krx_pct"),
    ], axis=1)

    # 0 = KRX sell/NXT buy, 1 = NXT sell/KRX buy
    best_direction_idx = best_spread.values.argmax(axis=1)
    best_direction = np.where(best_direction_idx == 0, "KRX sell / NXT buy", "NXT sell / KRX buy")
    best_spread_pct = best_spread.max(axis=1)

    out = merged.assign(
        sec=sec_series,
        krx_over_nxt_pct=krx_over_nxt.values,
        nxt_over_krx_pct=nxt_over_krx.values,
        best_direction=best_direction,
        best_spread_pct=best_spread_pct.values,
    ).sort_values("ts").reset_index(drop=True)

    # Column order
    cols = [
        "ts", "sec",
        "krx_min", "krx_max",
        "nxt_min", "nxt_max",
        "krx_over_nxt_pct", "nxt_over_krx_pct",
        "best_direction", "best_spread_pct",
    ]
    return out[cols]


# -----------------------------
# Tkinter GUI
# -----------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("KRX vs NXT Spread (per-second) — Tk GUI")
        self.geometry("1100x720")

        root = ttk.Frame(self, padding=10)
        root.pack(fill=tk.BOTH, expand=True)

        # File selection
        file_group = ttk.LabelFrame(root, text="Input Files")
        file_group.pack(fill=tk.X)

        self.krx_path = tk.StringVar()
        self.nxt_path = tk.StringVar()
        self.krx_sheet = tk.StringVar()
        self.nxt_sheet = tk.StringVar()

        self._row_file(file_group, 0, "KRX file (CSV/XLSX)", self.krx_path)
        self._row_file(file_group, 1, "NXT file (CSV/XLSX)", self.nxt_path)

        ttk.Label(file_group, text="KRX sheet").grid(row=2, column=0, sticky=tk.W, padx=6, pady=4)
        ttk.Entry(file_group, textvariable=self.krx_sheet, width=30).grid(row=2, column=1, sticky=tk.W, padx=6, pady=4)
        ttk.Label(file_group, text="NXT sheet").grid(row=3, column=0, sticky=tk.W, padx=6, pady=4)
        ttk.Entry(file_group, textvariable=self.nxt_sheet, width=30).grid(row=3, column=1, sticky=tk.W, padx=6, pady=4)

        # Options
        opt_group = ttk.LabelFrame(root, text="Options")
        opt_group.pack(fill=tk.X, pady=(8, 0))

        self.time_col = tk.StringVar()
        self.price_col = tk.StringVar()
        ttk.Label(opt_group, text="Timestamp column").grid(row=0, column=0, sticky=tk.W, padx=6, pady=4)
        ttk.Entry(opt_group, textvariable=self.time_col, width=30).grid(row=0, column=1, sticky=tk.W, padx=6, pady=4)
        ttk.Label(opt_group, text="Price column").grid(row=1, column=0, sticky=tk.W, padx=6, pady=4)
        ttk.Entry(opt_group, textvariable=self.price_col, width=30).grid(row=1, column=1, sticky=tk.W, padx=6, pady=4)

        ttk.Label(opt_group, text="CSV encoding").grid(row=2, column=0, sticky=tk.W, padx=6, pady=4)
        self.encoding = tk.StringVar(value="auto")
        enc_combo = ttk.Combobox(opt_group, textvariable=self.encoding, state="readonly",
                                 values=["auto", "utf-8-sig", "cp949", "euc-kr"])
        enc_combo.grid(row=2, column=1, sticky=tk.W, padx=6, pady=4)

        ttk.Label(opt_group, text="CSV chunksize").grid(row=3, column=0, sticky=tk.W, padx=6, pady=4)
        self.chunksize = tk.IntVar(value=500_000)
        ttk.Spinbox(opt_group, from_=10_000, to=2_000_000, increment=50_000, textvariable=self.chunksize, width=12).grid(row=3, column=1, sticky=tk.W, padx=6, pady=4)

        # Output
        out_group = ttk.LabelFrame(root, text="Output")
        out_group.pack(fill=tk.X, pady=(8, 0))

        self.out_path = tk.StringVar()
        self._row_save(out_group, 0, "Output CSV", self.out_path)

        # Run + status
        run_row = ttk.Frame(root)
        run_row.pack(fill=tk.X, pady=(8, 6))
        ttk.Button(run_row, text="Run", command=self.run_job).pack(side=tk.LEFT)
        self.status = tk.StringVar(value="Ready.")
        ttk.Label(run_row, textvariable=self.status, foreground="#2b7a0b").pack(side=tk.LEFT, padx=10)

        # Table preview
        table_group = ttk.LabelFrame(root, text="Preview (first 1,000 rows)")
        table_group.pack(fill=tk.BOTH, expand=True)
        self.tree = ttk.Treeview(table_group, columns=(), show="headings")
        self.tree.pack(fill=tk.BOTH, expand=True)
        # add scrollbars
        yscroll = ttk.Scrollbar(table_group, orient=tk.VERTICAL, command=self.tree.yview)
        xscroll = ttk.Scrollbar(table_group, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)
        yscroll.pack(side=tk.RIGHT, fill=tk.Y)
        xscroll.pack(side=tk.BOTTOM, fill=tk.X)

    def _row_file(self, parent, row, label, var: tk.StringVar):
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky=tk.W, padx=6, pady=4)
        ttk.Entry(parent, textvariable=var, width=80).grid(row=row, column=1, sticky=tk.W, padx=6, pady=4)
        ttk.Button(parent, text="Browse…", command=lambda: self._browse_file(var)).grid(row=row, column=2, padx=6)

    def _row_save(self, parent, row, label, var: tk.StringVar):
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky=tk.W, padx=6, pady=4)
        ttk.Entry(parent, textvariable=var, width=80).grid(row=row, column=1, sticky=tk.W, padx=6, pady=4)
        ttk.Button(parent, text="Save as…", command=lambda: self._browse_save(var)).grid(row=row, column=2, padx=6)

    def _browse_file(self, var: tk.StringVar):
        path = filedialog.askopenfilename(title="Select file", filetypes=[
            ("Data Files", "*.csv *.xlsx *.xls"), ("All Files", "*.*")
        ])
        if path:
            var.set(path)
            if var is self.krx_path and not self.out_path.get():
                base = os.path.splitext(os.path.basename(path))[0]
                self.out_path.set(os.path.join(os.path.dirname(path), f"{base}_초당_스프레드.csv"))

    def _browse_save(self, var: tk.StringVar):
        path = filedialog.asksaveasfilename(title="Save output CSV", defaultextension=".csv",
                                            initialfile=self.out_path.get() or "spreads.csv",
                                            filetypes=[("CSV Files", "*.csv")])
        if path:
            var.set(path)

    def _encoding_value(self) -> Optional[str]:
        val = self.encoding.get()
        return None if val == "auto" else val

    def _sheet_value(self, s: str) -> Optional[str | int]:
        s = s.strip()
        if not s:
            return None
        return int(s) if s.isdigit() else s

    def _set_status(self, text: str, color="#2b7a0b"):
        self.status.set(text)
        # ttk.Label doesn't support fg directly; using a new label is overkill, keep text only.
        # If you want colored text, use a tk.Label instead.

    def run_job(self):
        krx_path = self.krx_path.get().strip()
        nxt_path = self.nxt_path.get().strip()
        out_path = self.out_path.get().strip()
        time_col = self.time_col.get().strip() or None
        price_col = self.price_col.get().strip() or None
        encoding = self._encoding_value()
        chunksize = int(self.chunksize.get())
        krx_sheet = self._sheet_value(self.krx_sheet.get())
        nxt_sheet = self._sheet_value(self.nxt_sheet.get())

        if not krx_path or not os.path.exists(krx_path):
            messagebox.showwarning("Missing file", "Please select a valid KRX file.")
            return
        if not nxt_path or not os.path.exists(nxt_path):
            messagebox.showwarning("Missing file", "Please select a valid NXT file.")
            return
        if not out_path:
            messagebox.showwarning("Missing output", "Please choose an output CSV path.")
            return

        self._set_status("Running… this may take a while for large files.")
        self.update_idletasks()

        try:
            df = compute_cross_exchange_spreads(
                krx_path=krx_path,
                nxt_path=nxt_path,
                time_col=time_col,
                price_col=price_col,
                encoding=encoding,
                chunksize=chunksize,
                krx_sheet=krx_sheet,
                nxt_sheet=nxt_sheet,
            )
        except Exception as e:
            self._set_status("Error.")
            messagebox.critical(self, "Failed", f"Computation failed:\n{e}")
            return

        try:
            os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
            df.to_csv(out_path, index=False, encoding="utf-8-sig")
        except Exception as e:
            self._set_status("Save failed.")
            messagebox.critical(self, "Save failed", f"Could not save CSV:\n{e}")
            return

        # Update preview (first 1,000 rows)
        preview = df.head(1000)
        self._load_preview(preview)

        self._set_status(f"Done. Rows: {len(df):,}. Saved → {out_path}")

    def _load_preview(self, df: pd.DataFrame):
        # Clear existing
        for col in self.tree["columns"]:
            self.tree.heading(col, text="")
        self.tree.delete(*self.tree.get_children())

        # Configure new columns
        cols = list(df.columns)
        self.tree["columns"] = cols
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=120, anchor=tk.W)

        # Insert rows
        for _, row in df.iterrows():
            vals = []
            for v in row:
                if pd.isna(v):
                    vals.append("")
                elif isinstance(v, float):
                    vals.append(f"{v:.6f}")
                else:
                    vals.append(str(v))
            self.tree.insert("", tk.END, values=vals)


def main():
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
