"""
Build the unified TickerAtlas asset index: every stock, ETF and cryptocurrency
in one small, flat file that is easy for people and AI agents to consume.

Reads:
    data/All_Stocks.csv   (stocks + ETFs, NASDAQ Trader listings + Yahoo Finance metadata)
    data/Crypto.csv       (crypti/cryptocurrencies, MIT)

Writes:
    data/All_Assets.csv   id,ticker,name,asset_class,exchange,country,sector,source

`id` is "<asset_class>:<ticker>" and is unique. `ticker` alone is NOT unique
(e.g. "SOL" is both an NYSE-listed stock and the Solana cryptocurrency).
"""

import argparse
import os
from typing import Optional

import pandas as pd

ASSET_COLUMNS = [
    "id",
    "ticker",
    "name",
    "asset_class",
    "exchange",
    "country",
    "sector",
    "source",
]

# Yahoo quoteType -> TickerAtlas asset_class
QUOTE_TYPE_MAP = {
    "EQUITY": "stock",
    "ETF": "etf",
    "MUTUALFUND": "fund",
    "INDEX": "index",
    "CRYPTOCURRENCY": "crypto",
}


def read_csv_safe(path: str) -> pd.DataFrame:
    """Read a CSV keeping real tickers like "NA" or "NULL" as strings."""
    return pd.read_csv(path, keep_default_na=False, na_values=[""], dtype=str)


def asset_class_for(quote_type: Optional[str]) -> str:
    if not isinstance(quote_type, str) or not quote_type.strip():
        return "stock"
    q = quote_type.strip().upper()
    return QUOTE_TYPE_MAP.get(q, q.lower())


def load_stocks(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=ASSET_COLUMNS)
    df = read_csv_safe(path)
    df = df[df["ticker"].notna() & (df["ticker"].str.strip() != "")].copy()
    df["ticker"] = df["ticker"].str.strip()
    df["asset_class"] = df["quote_type"].apply(asset_class_for)
    out = pd.DataFrame(
        {
            "ticker": df["ticker"],
            "name": df["company_name"],
            "asset_class": df["asset_class"],
            "exchange": df.get("exchange_name", df.get("exchange")),
            "country": df.get("country"),
            "sector": df.get("sector"),
            "source": "nasdaqtrader+yfinance",
        }
    )
    return out


def load_crypto(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=ASSET_COLUMNS)
    df = read_csv_safe(path)
    df = df[df["ticker"].notna() & (df["ticker"] != "")]
    return pd.DataFrame(
        {
            "ticker": df["ticker"],
            "name": df["name"],
            "asset_class": "crypto",
            "exchange": None,
            "country": None,
            "sector": None,
            "source": df.get("source", "crypti/cryptocurrencies"),
        }
    )


def build_index(stocks_csv: str, crypto_csv: str) -> pd.DataFrame:
    df = pd.concat([load_stocks(stocks_csv), load_crypto(crypto_csv)], ignore_index=True)
    df["id"] = df["asset_class"] + ":" + df["ticker"]
    df = df.drop_duplicates(subset=["id"], keep="first")
    df = df.sort_values(["ticker", "asset_class"], key=lambda s: s.str.upper())
    return df[ASSET_COLUMNS].reset_index(drop=True)


def main() -> int:
    p = argparse.ArgumentParser(description="Build data/All_Assets.csv")
    p.add_argument("--stocks-csv", default="data/All_Stocks.csv")
    p.add_argument("--crypto-csv", default="data/Crypto.csv")
    p.add_argument("--out-csv", default="data/All_Assets.csv")
    args = p.parse_args()

    df = build_index(args.stocks_csv, args.crypto_csv)
    if df.empty:
        print("Error: no assets found (are data/All_Stocks.csv and data/Crypto.csv present?)")
        return 1
    os.makedirs(os.path.dirname(args.out_csv) or ".", exist_ok=True)
    df.to_csv(args.out_csv, index=False, lineterminator="\n")
    print(f"Wrote {args.out_csv}: {len(df)} assets")
    print(df["asset_class"].value_counts().to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
