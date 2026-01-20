import argparse
import datetime as dt
import json
import os
import re
import sqlite3
import time
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from yfinance.exceptions import YFRateLimitError

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover
    tqdm = None


NASDAQ_LISTED_URLS = [
    "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
    "https://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
]
OTHER_LISTED_URLS = [
    "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
    "https://ftp.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
]

STOOQ_DB_URL = "https://stooq.com/db/"


# Curated fallback list of Stooq "stock list" groups (used when /db/ is JS-driven)
STOOQ_STOCK_GROUPS: List[Tuple[int, str]] = [
    (27, "Nasdaq Stocks"),
    (28, "NYSE Stocks"),
    (26, "NYSE MKT Stocks"),
    (16, "LSE Stocks"),
    (23, "LSE International Stocks"),
    (34, "TSE ETFs"),
    (33, "TSE Indices"),
]


@dataclass(frozen=True)
class ListingSource:
    name: str
    url: str


def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _download_text(url: str, timeout_s: int = 60) -> str:
    s = requests.Session()
    r = s.get(
        url,
        timeout=timeout_s,
        headers={
            "User-Agent": "List-of-Stocks/1.0 (+https://github.com/)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    r.raise_for_status()
    return r.text


def _download_text_with_retries(url: str, timeout_s: int = 60, attempts: int = 3) -> str:
    last_err: Optional[BaseException] = None
    for i in range(attempts):
        try:
            return _download_text(url, timeout_s=timeout_s)
        except Exception as e:
            last_err = e
            time.sleep(2.0 * (i + 1))
    raise RuntimeError(f"Failed to download: {url}") from last_err


def fetch_nasdaq_trader_listings() -> pd.DataFrame:
    sources: List[ListingSource] = []
    for u in NASDAQ_LISTED_URLS:
        sources.append(ListingSource("nasdaqlisted", u))
    for u in OTHER_LISTED_URLS:
        sources.append(ListingSource("otherlisted", u))

    frames: List[pd.DataFrame] = []
    done = set()
    it = sources
    if tqdm is not None:
        it = tqdm(sources, desc="Fetch US listing files", unit="file")
    for src in it:
        if src.name in done:
            continue
        try:
            txt = _download_text_with_retries(src.url)
        except Exception:
            continue
        # Files are pipe-delimited and end with a summary row.
        lines = [ln for ln in txt.splitlines() if ln.strip()]
        header = lines[0].split("|")
        body = [ln.split("|") for ln in lines[1:] if not ln.startswith("File Creation Time")]
        df = pd.DataFrame(body, columns=header)

        # Remove the final summary row(s) like "File Creation Time" or "Number of..."
        df = df[df[header[0]].notna()]
        df = df[~df[header[0]].astype(str).str.contains("Number of", na=False)]

        df["listing_source"] = src.name
        frames.append(df)
        done.add(src.name)

    if not frames:
        raise RuntimeError(
            "Failed to download US listings from NASDAQ Trader (both www and ftp hosts). "
            "Try again later or run with --universe global to test the rest of the pipeline."
        )

    listings = pd.concat(frames, ignore_index=True)

    # Normalize key columns
    if "Symbol" in listings.columns:
        listings = listings.rename(columns={"Symbol": "ticker"})
    elif "ACT Symbol" in listings.columns:
        listings = listings.rename(columns={"ACT Symbol": "ticker"})

    listings["ticker"] = listings["ticker"].astype(str).str.strip()

    # Filter out test issues and invalid tickers
    listings = listings[listings["ticker"].str.len() > 0]
    listings = listings[~listings["ticker"].str.contains("\\^", regex=True, na=False)]

    return listings


def _stooq_extract_group_links(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    groups: List[Dict[str, str]] = []
    for a in soup.find_all("a"):
        href = a.get("href") or ""
        if not href.startswith("/db/l/?g="):
            continue
        label = (a.get_text() or "").strip()
        if not label:
            continue
        groups.append({"href": "https://stooq.com" + href, "label": label})

    # If the page is JS-driven, links may not exist as <a> tags. Try regex extraction.
    if not groups:
        for gid in sorted(set(re.findall(r"/db/l/\?g=(\d+)", html))):
            groups.append({"href": f"https://stooq.com/db/l/?g={gid}", "label": f"g={gid}"})
    # De-dup
    seen = set()
    out: List[Dict[str, str]] = []
    for g in groups:
        if g["href"] in seen:
            continue
        seen.add(g["href"])
        out.append(g)
    return out


def _stooq_parse_group_table(html: str) -> pd.DataFrame:
    # Many Stooq list pages contain a simple HTML table.
    try:
        tables = pd.read_html(StringIO(html))
    except Exception:
        tables = []

    if tables:
        df = tables[0]
        cols = [str(c).strip().lower() for c in df.columns]
        if len(cols) >= 1:
            df = df.rename(columns={df.columns[0]: "ticker"})
        if len(cols) >= 2:
            df = df.rename(columns={df.columns[1]: "stooq_name"})
        if "ticker" in df.columns:
            df["ticker"] = df["ticker"].astype(str).str.strip()
        return df

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if table is None:
        return pd.DataFrame()
    rows = table.find_all("tr")
    parsed: List[Dict[str, str]] = []
    for tr in rows:
        tds = tr.find_all(["td", "th"])
        if len(tds) < 1:
            continue
        sym = (tds[0].get_text() or "").strip()
        if not sym or sym.lower() in ("symbol", "sym"):
            continue
        name = (tds[1].get_text() or "").strip() if len(tds) >= 2 else ""
        parsed.append({"ticker": sym, "stooq_name": name})
    if not parsed:
        return pd.DataFrame()
    return pd.DataFrame(parsed)


def fetch_stooq_global_listings(timeout_s: int = 60, verbose: bool = False) -> pd.DataFrame:
    # Best-effort global universe by scraping Stooq's database group pages.
    index_html = _download_text_with_retries(STOOQ_DB_URL, timeout_s=timeout_s)
    groups = _stooq_extract_group_links(index_html)

    if verbose:
        print(f"Stooq index groups_found={len(groups)}")
        if len(groups) == 0:
            snippet = index_html[:500].replace("\n", " ")
            print(f"Stooq index snippet={snippet}")

    # Fallback: use a curated set of group IDs if index provides no usable links.
    if not groups:
        groups = [
            {"href": f"https://stooq.com/db/l/?g={gid}", "label": label}
            for gid, label in STOOQ_STOCK_GROUPS
        ]
        if verbose:
            print(f"Stooq fallback groups_used={len(groups)}")

    frames: List[pd.DataFrame] = []
    it = groups
    if tqdm is not None:
        it = tqdm(groups, desc="Fetch global listing groups", unit="group")
    for g in it:
        label = g["label"]
        # Only keep groups that look like stock lists
        if "stocks" not in label.lower():
            continue

        try:
            html = _download_text_with_retries(g["href"], timeout_s=timeout_s)
            df = _stooq_parse_group_table(html)
            if df.empty:
                if verbose:
                    print(f"Stooq group empty label={label} url={g['href']}")
                continue
            df["listing_source"] = "stooq"
            df["listing_group"] = label
            frames.append(df[["ticker", "listing_source", "listing_group"]])
        except Exception:
            continue

    if not frames:
        return pd.DataFrame(columns=["ticker", "listing_source", "listing_group"])

    out = pd.concat(frames, ignore_index=True)
    out = out.dropna(subset=["ticker"])
    out["ticker"] = out["ticker"].astype(str).str.strip()
    out = out[out["ticker"].str.len() > 0]
    out = out.drop_duplicates(subset=["ticker"], keep="first")
    return out


def fetch_global_listings_from_file(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=["ticker", "listing_source", "listing_group"])
    with open(path, "r", encoding="utf-8") as f:
        tickers = [ln.strip() for ln in f.read().splitlines()]
    tickers = [t for t in tickers if t and not t.startswith("#")]
    df = pd.DataFrame({"ticker": tickers})
    df["listing_source"] = "file"
    df["listing_group"] = os.path.basename(path)
    df["ticker"] = df["ticker"].astype(str).str.strip()
    df = df[df["ticker"].str.len() > 0]
    df = df.drop_duplicates(subset=["ticker"], keep="first")
    return df


def _yf_info_for_ticker(ticker: str) -> Dict[str, Any]:
    t = yf.Ticker(ticker)
    info = t.info or {}
    # Ensure plain json-serializable values
    out: Dict[str, Any] = {}
    for k, v in info.items():
        try:
            json.dumps(v)
            out[k] = v
        except TypeError:
            out[k] = str(v)
    return out


def enrich_with_yfinance(
    tickers: Iterable[str],
    sleep_s: float = 0.4,
    max_tickers: Optional[int] = None,
    workers: int = 8,
    max_retries: int = 6,
    start: int = 0,
    count: Optional[int] = None,
) -> pd.DataFrame:
    tickers_list = [str(t).strip() for t in tickers if str(t).strip()]
    if start < 0:
        start = 0
    if count is not None and count < 0:
        count = None
    if count is not None:
        tickers_list = tickers_list[start : start + count]
    else:
        tickers_list = tickers_list[start:]
    if max_tickers is not None:
        tickers_list = tickers_list[: max_tickers]

    rows: List[Dict[str, Any]] = []

    def fetch_one(t: str) -> Dict[str, Any]:
        delay = max(0.0, float(sleep_s))
        for attempt in range(max_retries):
            try:
                info = _yf_info_for_ticker(t)
                info["ticker"] = t
                if delay > 0:
                    time.sleep(delay)
                return info
            except YFRateLimitError:
                # Exponential backoff on Yahoo throttling.
                backoff = min(300.0, 5.0 * (2**attempt))
                time.sleep(backoff)
            except Exception:
                # Other transient errors: small backoff and retry.
                time.sleep(min(30.0, 1.5 * (attempt + 1)))

        return {"ticker": t}

    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as ex:
        futures = [ex.submit(fetch_one, t) for t in tickers_list]
        it = as_completed(futures)
        if tqdm is not None:
            it = tqdm(it, total=len(futures), desc="yfinance metadata", unit="ticker")
        for fut in it:
            rows.append(fut.result())

    if not rows:
        return pd.DataFrame(columns=["ticker"])

    df = pd.DataFrame(rows)
    if "ticker" not in df.columns:
        df["ticker"] = []
    return df


def _epoch_to_date_str(epoch: Any) -> Optional[str]:
    if epoch is None:
        return None
    try:
        epoch_int = int(epoch)
        return dt.datetime.fromtimestamp(epoch_int, tz=dt.timezone.utc).date().isoformat()
    except Exception:
        return None


def normalize_schema(listings: pd.DataFrame, yf_info: pd.DataFrame) -> pd.DataFrame:
    merged = listings.merge(yf_info, on="ticker", how="left", suffixes=("", "_yf"))

    now = dt.datetime.now(dt.timezone.utc).date()

    def col(name: str) -> pd.Series:
        if name in merged.columns:
            return merged[name]
        return pd.Series([None] * len(merged))

    def lcol(name: str) -> pd.Series:
        # Listing-side columns can be missing depending on source file.
        if name in merged.columns:
            return merged[name]
        return pd.Series([None] * len(merged))

    def _map_otherlisted_exchange(x: Any) -> Optional[str]:
        # otherlisted.txt uses single-letter exchange codes.
        if x is None:
            return None
        s = str(x).strip().upper()
        m = {
            "A": "NYSE American",
            "N": "NYSE",
            "P": "NYSE Arca",
            "Z": "Cboe BZX",
            "V": "IEX",
        }
        return m.get(s, None)

    def _map_nasdaq_market_category(x: Any) -> Optional[str]:
        # nasdaqlisted.txt Market Category: Q=Global Select, G=Global Market, S=Capital Market
        if x is None:
            return None
        s = str(x).strip().upper()
        m = {
            "Q": "Nasdaq Global Select",
            "G": "Nasdaq Global Market",
            "S": "Nasdaq Capital Market",
        }
        return m.get(s, None)

    first_trade_date = merged.get("firstTradeDateEpochUtc")
    if first_trade_date is not None:
        merged["first_trade_date"] = first_trade_date.apply(_epoch_to_date_str)
    else:
        merged["first_trade_date"] = None

    def age_years(d: Any) -> Optional[float]:
        if not d:
            return None
        try:
            dd = dt.date.fromisoformat(str(d))
            return round((now - dd).days / 365.25, 2)
        except Exception:
            return None

    merged["age_years_since_first_trade"] = merged["first_trade_date"].apply(age_years)

    listing_company = lcol("Security Name")
    if listing_company.isna().all():
        listing_company = lcol("Company Name")

    # Listing-side exchange fallback
    listing_exchange = lcol("Exchange").apply(_map_otherlisted_exchange)
    if listing_exchange.isna().all():
        listing_exchange = lcol("Market Category").apply(_map_nasdaq_market_category)

    # Listing-side quote_type fallback (ETF flag is a good signal)
    etf_flag = lcol("ETF").astype(str).str.strip().str.upper()
    listing_quote_type = pd.Series([None] * len(merged))
    listing_quote_type.loc[etf_flag == "Y"] = "ETF"

    # Canonical fields for downstream use
    out = pd.DataFrame(
        {
            "ticker": merged["ticker"],
            "company_name": col("longName").fillna(col("shortName")).fillna(listing_company),
            "quote_type": col("quoteType").fillna(listing_quote_type),
            "exchange": col("exchange").fillna(col("fullExchangeName")).fillna(listing_exchange),
            "exchange_name": col("fullExchangeName").fillna(listing_exchange),
            "currency": col("currency"),
            "country": col("country"),
            "sector": col("sector"),
            "industry": col("industry"),
            "website": col("website"),
            "employees": col("fullTimeEmployees"),
            "market_cap": col("marketCap"),
            "first_trade_date": merged.get("first_trade_date"),
            "age_years_since_first_trade": merged.get("age_years_since_first_trade"),
            "business_summary": col("longBusinessSummary"),
            "listing_source": col("listing_source"),
            "updated_at_utc": _utc_now_iso(),
        }
    )

    # Drop clearly invalid tickers
    out = out[out["ticker"].notna()]
    out["ticker"] = out["ticker"].astype(str).str.strip()
    out = out[out["ticker"].str.len() > 0]

    # De-dup tickers (prefer rows with company_name)
    out = out.sort_values(by=["ticker", "company_name"], na_position="last")
    out = out.drop_duplicates(subset=["ticker"], keep="first")

    return out


def read_existing_tickers(path: str) -> set:
    """Read existing tickers from file if it exists, return empty set otherwise."""
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return {line.strip() for line in f.read().splitlines() if line.strip() and not line.startswith("#")}
        except Exception:
            return set()
    return set()


def write_tickers_txt(df: pd.DataFrame, path: str) -> None:
    _ensure_dir(os.path.dirname(path))
    tickers = df["ticker"].dropna().astype(str).unique()
    with open(path, "w", encoding="utf-8") as f:
        for t in tickers:
            f.write(t + "\n")


def read_existing_csv(path: str) -> pd.DataFrame:
    """Read existing CSV file if it exists, return empty DataFrame otherwise."""
    if os.path.exists(path):
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def merge_with_existing_data(new_data: pd.DataFrame, existing_data: pd.DataFrame) -> pd.DataFrame:
    """Merge new data with existing data, preferring new data for duplicates."""
    if existing_data.empty:
        return new_data
    
    # Combine existing and new data
    combined = pd.concat([existing_data, new_data], ignore_index=True)
    
    # Remove rows with empty tickers
    combined = combined[combined["ticker"].notna() & (combined["ticker"].astype(str).str.strip() != "")]
    combined["ticker"] = combined["ticker"].astype(str).str.strip()
    
    # Sort by ticker and updated_at_utc to prefer newer records
    combined['updated_at_utc'] = pd.to_datetime(combined['updated_at_utc'], errors='coerce')
    combined = combined.sort_values(['ticker', 'updated_at_utc'], na_position='last')
    
    # Drop duplicates, keeping the first (newest) record
    combined = combined.drop_duplicates(subset=['ticker'], keep='first')
    
    return combined


def write_csv(df: pd.DataFrame, path: str) -> None:
    _ensure_dir(os.path.dirname(path))
    df.to_csv(path, index=False)


def write_sqlite(df: pd.DataFrame, path: str) -> None:
    _ensure_dir(os.path.dirname(path))
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path)
    try:
        df.to_sql("stocks", conn, if_exists="replace", index=False)
        conn.execute("CREATE INDEX idx_stocks_ticker ON stocks(ticker)")
        conn.commit()
    finally:
        conn.close()


def write_sqlite_upsert(df: pd.DataFrame, path: str) -> None:
    """Write or upsert into SQLite with a primary key on ticker."""
    _ensure_dir(os.path.dirname(path))
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    # Ensure the table exists with a primary key on ticker
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            ticker TEXT PRIMARY KEY,
            company_name TEXT,
            quote_type TEXT,
            exchange TEXT,
            exchange_name TEXT,
            currency TEXT,
            country TEXT,
            sector TEXT,
            industry TEXT,
            website TEXT,
            employees INTEGER,
            market_cap REAL,
            first_trade_date TEXT,
            age_years_since_first_trade REAL,
            business_summary TEXT,
            listing_source TEXT,
            updated_at_utc TEXT
        )
    """)
    # Upsert each row
    for _, row in df.iterrows():
        cols = list(df.columns)
        placeholders = ", ".join(["?"] * len(cols))
        update_assign = ", ".join([f"{c}=excluded.{c}" for c in cols if c != "ticker"])
        sql = f"""
            INSERT INTO stocks ({", ".join(cols)})
            VALUES ({placeholders})
            ON CONFLICT(ticker) DO UPDATE SET {update_assign}
        """
        try:
            cur.execute(sql, tuple(row[c] for c in cols))
        except sqlite3.OperationalError as e:
            # If table exists without primary key or missing columns, recreate it
            if "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint" in str(e) or "has no column named" in str(e):
                cur.execute("DROP TABLE stocks")
                cur.execute("""
                    CREATE TABLE stocks (
                        ticker TEXT PRIMARY KEY,
                        company_name TEXT,
                        quote_type TEXT,
                        exchange TEXT,
                        exchange_name TEXT,
                        currency TEXT,
                        country TEXT,
                        sector TEXT,
                        industry TEXT,
                        website TEXT,
                        employees INTEGER,
                        market_cap REAL,
                        first_trade_date TEXT,
                        age_years_since_first_trade REAL,
                        business_summary TEXT,
                        listing_source TEXT,
                        updated_at_utc TEXT
                    )
                """)
                cur.execute(sql, tuple(row[c] for c in cols))
            else:
                raise
    conn.commit()
    conn.close()


def export_csv_from_sqlite(db_path: str, csv_path: str) -> None:
    """Export the full SQLite table to CSV."""
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM stocks ORDER BY ticker", conn)
    conn.close()
    df.to_csv(csv_path, index=False)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--out-dir", default="data")
    p.add_argument("--sleep-s", type=float, default=0.8)
    p.add_argument("--max-tickers", type=int, default=None)
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--max-retries", type=int, default=6)
    p.add_argument("--no-yfinance", action="store_true")
    p.add_argument("--start", type=int, default=0, help="Start index into the ticker list (for chunked runs)")
    p.add_argument("--count", type=int, default=None, help="Number of tickers to process from --start")
    p.add_argument("--universe", choices=["us", "global", "all"], default="us")
    p.add_argument("--global-source", choices=["file", "stooq"], default="file")
    p.add_argument("--global-file", default=os.path.join("inputs", "global_tickers.txt"))
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--db-mode", choices=["replace", "upsert"], default="replace", help="SQLite write mode: replace (default) or upsert (persistent across runs)")
    args = p.parse_args()

    universes: List[Tuple[str, pd.DataFrame]] = []
    if args.universe in ("us", "all"):
        universes.append(("us", fetch_nasdaq_trader_listings()))
    if args.universe in ("global", "all"):
        if args.global_source == "stooq":
            universes.append(("global", fetch_stooq_global_listings(verbose=args.verbose)))
        else:
            universes.append(("global", fetch_global_listings_from_file(args.global_file)))

    if args.verbose:
        print(f"Starting build: universe={args.universe} global_source={args.global_source}")

    for name, listings in universes:
        if "ticker" not in listings.columns:
            raise RuntimeError(f"Universe '{name}' listings did not include a ticker column")
        tickers = listings["ticker"].dropna().astype(str).unique().tolist()
        if args.verbose:
            print(f"Universe={name} listings_rows={len(listings)} unique_tickers={len(tickers)}")
            if not args.no_yfinance:
                if args.count is None:
                    print(f"Universe={name} yfinance chunk start={args.start} count=ALL")
                else:
                    print(f"Universe={name} yfinance chunk start={args.start} count={args.count}")
        if not tickers:
            if name == "global" and args.global_source == "file":
                print(
                    "Skipping global build: inputs/global_tickers.txt is empty or missing. "
                    "Add tickers there (one per line) to enable global outputs."
                )
                continue
            raise RuntimeError(
                f"Universe '{name}' produced 0 tickers. "
                "If you're running global with --global-source file, populate inputs/global_tickers.txt. "
                "If you're running global with --global-source stooq, the site may be blocked/unreachable right now."
            )
        if args.no_yfinance:
            yf_info = pd.DataFrame({"ticker": tickers})
        else:
            yf_info = enrich_with_yfinance(
                tickers,
                sleep_s=args.sleep_s,
                max_tickers=args.max_tickers,
                workers=args.workers,
                max_retries=args.max_retries,
                start=args.start,
                count=args.count,
            )
        if "ticker" not in yf_info.columns:
            raise RuntimeError("yfinance enrichment returned no ticker column")
        if args.verbose:
            print(f"Universe={name} normalizing")
        normalized = normalize_schema(listings, yf_info)

        prefix = "US" if name == "us" else "Global"
        out_csv = os.path.join(args.out_dir, f"{prefix}_Stocks.csv")
        out_db = os.path.join(args.out_dir, f"{prefix}_Stocks.sqlite")
        out_txt = os.path.join(args.out_dir, f"{prefix}_Tickers.txt")

        if args.verbose:
            print(f"Universe={name} writing CSV: {out_csv}")
        
        # Read existing data and merge with new data to preserve existing stocks
        existing_data = read_existing_csv(out_csv)
        merged_data = merge_with_existing_data(normalized, existing_data)
        write_csv(merged_data, out_csv)
        if args.verbose:
            print(f"Universe={name} writing SQLite: {out_db}")
        if args.db_mode == "upsert":
            write_sqlite_upsert(normalized, out_db)
            export_csv_from_sqlite(out_db, out_csv)
            if args.verbose:
                print(f"Universe={name} exported CSV from DB (upsert mode): {out_csv}")
        else:
            write_sqlite(normalized, out_db)
        if args.verbose:
            print(f"Universe={name} writing tickers: {out_txt}")
        
        # Merge with existing tickers to preserve them
        existing_tickers = read_existing_tickers(out_txt)
        new_tickers = set(normalized["ticker"].dropna().astype(str).unique())
        all_tickers = existing_tickers.union(new_tickers)
        
        # Create a DataFrame with all tickers for writing
        all_tickers_df = pd.DataFrame({"ticker": sorted(all_tickers)})
        write_tickers_txt(all_tickers_df, out_txt)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
