import tkinter as tk
from tkinter import filedialog
import pandas as pd
import yaml
from dataclasses import dataclass
from collections import defaultdict
from pathlib import Path

@dataclass
class QuoteSnapshot:
    krx_bid: int = 0
    krx_ask: int = 0
    krx_bid_size: int = 0
    krx_ask_size: int = 0
    nxt_bid: int = 0
    nxt_ask: int = 0
    nxt_bid_size: int = 0
    nxt_ask_size: int = 0


def load_config(path: Path) -> dict:
    with open(path, 'r') as f:
        return yaml.safe_load(f)


def get_tick_size(price: float) -> int:
    if price < 2000:
        return 1
    elif price < 5000:
        return 5
    elif price < 20000:
        return 10
    elif price < 50000:
        return 50
    elif price < 200000:
        return 100
    elif price < 500000:
        return 500
    else:
        return 1000


def is_quote_valid(q: QuoteSnapshot, min_visible: int) -> bool:
    krx_valid = (
        q.krx_bid > 0 and q.krx_ask > 0 and q.krx_ask > q.krx_bid and
        min(q.krx_bid_size, q.krx_ask_size) >= min_visible
    )
    nxt_valid = (
        q.nxt_bid > 0 and q.nxt_ask > 0 and q.nxt_ask > q.nxt_bid and
        min(q.nxt_bid_size, q.nxt_ask_size) >= min_visible
    )
    return krx_valid and nxt_valid


def calculate_direction_edge(symbol: str, buy_venue: str, buy_price: int, buy_size: int,
                              sell_venue: str, sell_price: int, sell_size: int,
                              fees: dict) -> dict | None:
    if sell_price <= buy_price:
        return None

    gross_edge = sell_price - buy_price
    buy_fees = buy_price * fees[buy_venue] / 10000
    sell_fees = sell_price * fees[sell_venue] / 10000
    total_fees = buy_fees + sell_fees
    net_edge = gross_edge - total_fees
    edge_bps = net_edge / buy_price * 10000 if buy_price else 0
    max_qty = min(buy_size, sell_size)

    return {
        'symbol': symbol,
        'buy_venue': buy_venue,
        'sell_venue': sell_venue,
        'buy_price': buy_price,
        'sell_price': sell_price,
        'edge_krw': gross_edge,
        'total_fees_krw': total_fees,
        'net_edge_krw': net_edge,
        'edge_bps': edge_bps,
        'max_qty': max_qty,
    }


def meets_threshold(signal: dict, q: QuoteSnapshot, min_ticks: int) -> bool:
    krx_mid = (q.krx_ask + q.krx_bid) / 2 if q.krx_ask and q.krx_bid else 0
    nxt_mid = (q.nxt_ask + q.nxt_bid) / 2 if q.nxt_ask and q.nxt_bid else 0
    if krx_mid and nxt_mid:
        ref = (krx_mid + nxt_mid) / 2
    else:
        ref = krx_mid or nxt_mid
    tick = get_tick_size(ref) if ref else 10
    return signal['net_edge_krw'] >= tick * min_ticks


def process_file(path: Path, config: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.read_excel(path)
    df['timestamp'] = pd.to_datetime(df['timestamp'].astype(str).str.replace("'", ''), errors='coerce')
    df = df.sort_values('timestamp')

    fees = {
        'KRX': config['fees']['krx']['broker_bps'],
        'NXT': config['fees']['nxt']['broker_bps'] + config['fees']['nxt'].get('regulatory_bps', 0),
    }
    min_ticks = config['spread_engine']['edge_rule']['min_net_ticks_after_fees']
    min_visible = config['spread_engine']['edge_rule']['also_require_min_visible_qty']

    quotes = defaultdict(QuoteSnapshot)
    opps: list[dict] = []

    for row in df.itertuples(index=False):
        symbol = str(row.symbol)
        q = quotes[symbol]
        ask = int(getattr(row, 'fid_41') or 0)
        bid = int(getattr(row, 'fid_51') or 0)
        ask_size = int(getattr(row, 'fid_61') or 0)
        bid_size = int(getattr(row, 'fid_71') or 0)

        if row.venue == 'KRX':
            q.krx_ask, q.krx_bid = ask, bid
            q.krx_ask_size, q.krx_bid_size = ask_size, bid_size
        else:
            q.nxt_ask, q.nxt_bid = ask, bid
            q.nxt_ask_size, q.nxt_bid_size = ask_size, bid_size

        if not is_quote_valid(q, min_visible):
            continue

        s1 = calculate_direction_edge(symbol, 'KRX', q.krx_ask, q.krx_ask_size,
                                      'NXT', q.nxt_bid, q.nxt_bid_size, fees)
        s2 = calculate_direction_edge(symbol, 'NXT', q.nxt_ask, q.nxt_ask_size,
                                      'KRX', q.krx_bid, q.krx_bid_size, fees)

        candidates = [s for s in (s1, s2) if s]
        if not candidates:
            continue
        best = max(candidates, key=lambda s: s['net_edge_krw'])
        if meets_threshold(best, q, min_ticks):
            record = row._asdict() | best
            opps.append(record)

    cols = list(df.columns) + ['buy_venue','sell_venue','buy_price','sell_price','edge_krw','total_fees_krw','net_edge_krw','edge_bps','max_qty']
    opp_df = pd.DataFrame(opps, columns=cols)
    return df, opp_df


def main():
    root = tk.Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(title='Select input Excel file', filetypes=[('Excel files', '*.xlsx')])
    if not file_path:
        return

    # Load configuration relative to this script's location so that
    # it works regardless of the current working directory.
    base_dir = Path(__file__).resolve().parent
    config = load_config(base_dir / 'config' / 'config.yaml')
    df, opp_df = process_file(Path(file_path), config)

    output_path = Path(file_path).with_name(Path(file_path).stem + '_with_opps.xlsx')
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='data')
        opp_df.to_excel(writer, index=False, sheet_name='opportunities')
    print(f'Saved {output_path} with {len(opp_df)} opportunities')

if __name__ == '__main__':
    main()