"""Utility for loading and saving symbol universes.

This module provides a small helper class that persists the list of
KRX tickers in an Excel spreadsheet.  The default location for the
spreadsheet is ``data/symbols/ticker_universe.xlsx`` relative to the
repository root, but a custom path can also be supplied.

The Excel file is expected to contain a single column named
``Ticker``.  Rows are read as strings and left‑padded with zeros so
that all tickers are six characters long.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional, Union

import pandas as pd


class SymbolLoader:
    """Load and persist symbol universes from Excel files."""

    def __init__(self, file_path: Optional[Union[str, Path]] = None) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        default_path = repo_root / "data" / "symbols" / "ticker_universe.xlsx"
        self.file_path = Path(file_path) if file_path else default_path

    # ------------------------------------------------------------------
    def load_symbols(self, file_path: Optional[Union[str, Path]] = None) -> List[str]:
        """Return a list of tickers from ``file_path``.

        Parameters
        ----------
        file_path:
            Optional override for the path from which to load symbols.

        Returns
        -------
        list[str]
            List of ticker strings.
        """

        path = Path(file_path) if file_path else self.file_path
        if not path.exists():
            raise FileNotFoundError(f"Symbol file not found: {path}")

        df = pd.read_excel(path)
        if "Ticker" not in df.columns:
            raise ValueError("Symbol file must contain a 'Ticker' column")

        tickers = (
            df["Ticker"].dropna().astype(str).str.zfill(6).tolist()
        )
        return tickers

    # ------------------------------------------------------------------
    def save_symbols(self, symbols: Iterable[str], file_path: Optional[Union[str, Path]] = None) -> Path:
        """Persist ``symbols`` to an Excel file.

        The target directory is created if it does not yet exist.

        Parameters
        ----------
        symbols:
            Iterable of ticker strings to save.
        file_path:
            Optional override for the path to save the symbols to.

        Returns
        -------
        Path
            The path of the file that was written.
        """

        path = Path(file_path) if file_path else self.file_path
        path.parent.mkdir(parents=True, exist_ok=True)

        df = pd.DataFrame({"Ticker": list(symbols)})
        df.to_excel(path, index=False)

        return path


__all__ = ["SymbolLoader"]