#!/usr/bin/env python3
"""
Validate and clean ticker symbols for Yahoo Finance compatibility.
Removes invalid tickers that consistently fail to fetch data.
"""
import argparse
import os
from typing import Set

import pandas as pd


def load_valid_tickers_from_csv(csv_path: str) -> Set[str]:
    """Load tickers that have company information (valid tickers)."""
    if not os.path.exists(csv_path):
        return set()

    df = pd.read_csv(csv_path)
    # Tickers with company_name are valid
    valid = (
        df[(df["company_name"].notna()) & (df["company_name"] != "")]["ticker"]
        .dropna()
        .unique()
    )
    return set(str(t).strip() for t in valid if t)


def remove_invalid_tickers(csv_path: str, db_path: str, txt_path: str) -> int:
    """Remove tickers without company information from all files."""

    # Load valid tickers
    valid_tickers = load_valid_tickers_from_csv(csv_path)

    if not valid_tickers:
        print("ERROR: No valid tickers found in CSV")
        return 1

    print(f"Found {len(valid_tickers)} valid tickers with company information")

    # Filter CSV
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        original_count = len(df)
        df = df[df["ticker"].isin(valid_tickers)]
        df.to_csv(csv_path, index=False)
        print(f"CSV: Removed {original_count - len(df)} invalid tickers")

    # Filter SQLite
    if os.path.exists(db_path):
        import sqlite3

        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        # Get current count
        cur.execute("SELECT COUNT(*) FROM stocks")
        original_count = cur.fetchone()[0]

        # Delete invalid tickers
        placeholders = ",".join(["?" for _ in valid_tickers])
        cur.execute(
            f"DELETE FROM stocks WHERE ticker NOT IN ({placeholders})",
            list(valid_tickers),
        )
        conn.commit()

        # Get new count
        cur.execute("SELECT COUNT(*) FROM stocks")
        new_count = cur.fetchone()[0]

        conn.close()
        print(f"SQLite: Removed {original_count - new_count} invalid tickers")

    # Filter TXT
    if os.path.exists(txt_path):
        with open(txt_path, "w", encoding="utf-8") as f:
            for ticker in sorted(valid_tickers):
                f.write(ticker + "\n")
        print(f"TXT: Updated with {len(valid_tickers)} valid tickers")

    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Validate and clean ticker symbols")
    p.add_argument("--csv", default="data/All_Stocks.csv", help="Path to CSV file")
    p.add_argument("--db", default="data/All_Stocks.sqlite", help="Path to SQLite DB")
    p.add_argument("--txt", default="data/All_Tickers.txt", help="Path to TXT file")
    args = p.parse_args()

    return remove_invalid_tickers(args.csv, args.db, args.txt)


if __name__ == "__main__":
    import sys

    sys.exit(main())
