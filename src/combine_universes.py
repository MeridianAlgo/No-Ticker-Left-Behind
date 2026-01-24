"""
Combine US and Global stock universes into a single comprehensive list with detailed company information.
"""

import argparse
import os
import sqlite3

import pandas as pd


def combine_universes(
    us_csv: str,
    global_csv: str,
    out_csv: str,
    out_db: str,
    out_txt: str,
) -> int:
    """
    Combine US and Global stock universes into a single dataset.

    Args:
        us_csv: Path to US_Stocks.csv
        global_csv: Path to Global_Stocks.csv
        out_csv: Output combined CSV path
        out_db: Output combined SQLite DB path
        out_txt: Output combined tickers text file path

    Returns:
        0 on success, 1 on failure
    """

    # Read both universes
    us_data = pd.DataFrame()
    global_data = pd.DataFrame()

    if os.path.exists(us_csv):
        try:
            us_data = pd.read_csv(us_csv)
            print(f"Loaded US universe: {len(us_data)} stocks")
        except Exception as e:
            print(f"Error reading US CSV: {e}")
            return 1

    if os.path.exists(global_csv):
        try:
            global_data = pd.read_csv(global_csv)
            print(f"Loaded Global universe: {len(global_data)} stocks")
        except Exception as e:
            print(f"Error reading Global CSV: {e}")
            return 1

    if us_data.empty and global_data.empty:
        print("Error: Both US and Global CSV files are empty or missing")
        return 1

    # Combine the datasets
    combined = pd.concat([us_data, global_data], ignore_index=True)

    # Remove duplicates, keeping the first occurrence
    combined = combined.drop_duplicates(subset=["ticker"], keep="first")

    # Sort by ticker
    combined = combined.sort_values("ticker").reset_index(drop=True)

    print(f"Combined universe: {len(combined)} unique stocks")

    # Write combined CSV
    os.makedirs(os.path.dirname(out_csv) or ".", exist_ok=True)
    combined.to_csv(out_csv, index=False)
    print(f"Wrote combined CSV: {out_csv}")

    # Write combined SQLite database
    conn = sqlite3.connect(out_db)
    try:
        combined.to_sql("stocks", conn, if_exists="replace", index=False)
        conn.execute("CREATE INDEX idx_stocks_ticker ON stocks(ticker)")
        conn.execute("CREATE INDEX idx_stocks_exchange ON stocks(exchange)")
        conn.execute("CREATE INDEX idx_stocks_sector ON stocks(sector)")
        conn.execute("CREATE INDEX idx_stocks_country ON stocks(country)")
        conn.commit()
        print(f"Wrote combined SQLite DB: {out_db}")
    finally:
        conn.close()

    # Write combined tickers text file
    tickers = combined["ticker"].dropna().astype(str).unique()
    os.makedirs(os.path.dirname(out_txt) or ".", exist_ok=True)
    with open(out_txt, "w", encoding="utf-8") as f:
        f.write("# Combined US and Global stock universe\n")
        f.write(f"# Total: {len(tickers)} unique tickers\n")
        for ticker in sorted(tickers):
            f.write(ticker + "\n")
    print(f"Wrote combined tickers: {out_txt}")

    # Print summary statistics
    print("\n=== Combined Universe Summary ===")
    print(f"Total stocks: {len(combined)}")
    print(f"Countries: {combined['country'].nunique()}")
    print(f"Sectors: {combined['sector'].nunique()}")
    print(f"Exchanges: {combined['exchange'].nunique()}")

    if "sector" in combined.columns:
        print("\nTop sectors:")
        sector_counts = combined["sector"].value_counts().head(10)
        for sector, count in sector_counts.items():
            print(f"  {sector}: {count}")

    if "country" in combined.columns:
        print("\nTop countries:")
        country_counts = combined["country"].value_counts().head(10)
        for country, count in country_counts.items():
            print(f"  {country}: {count}")

    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Combine US and Global stock universes")
    p.add_argument(
        "--us-csv", default="data/US_Stocks.csv", help="Path to US_Stocks.csv"
    )
    p.add_argument(
        "--global-csv",
        default="data/Global_Stocks.csv",
        help="Path to Global_Stocks.csv",
    )
    p.add_argument(
        "--out-csv", default="data/All_Stocks.csv", help="Output combined CSV"
    )
    p.add_argument(
        "--out-db", default="data/All_Stocks.sqlite", help="Output combined SQLite DB"
    )
    p.add_argument(
        "--out-txt", default="data/All_Tickers.txt", help="Output combined tickers text"
    )
    args = p.parse_args()

    return combine_universes(
        us_csv=args.us_csv,
        global_csv=args.global_csv,
        out_csv=args.out_csv,
        out_db=args.out_db,
        out_txt=args.out_txt,
    )


if __name__ == "__main__":
    raise SystemExit(main())
