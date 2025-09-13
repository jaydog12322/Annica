"""Placeholder module for Volatility Interruption (VI) symbol tracking.

This module will eventually interface with a real-time feed to maintain a set of
symbols currently subject to a volatility interruption (VI) halt.  For now it
provides a minimal in-memory list and query method so other components can be
wired to it.
"""
from typing import Iterable, Set


class VILister:
    """Simple placeholder VI lister."""

    def __init__(self) -> None:
        self._vi_symbols: Set[str] = set()

    def update(self, symbols: Iterable[str]) -> None:
        """Replace the current VI symbol set.

        Args:
            symbols: Iterable of symbols currently in VI.
        """
        self._vi_symbols = set(symbols)

    def is_in_vi(self, symbol: str) -> bool:
        """Check if *symbol* is marked as in VI."""
        return symbol in self._vi_symbols