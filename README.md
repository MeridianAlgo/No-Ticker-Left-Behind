# NTLB (No-Ticker-Left-Behind)

This repository contians all tickers for all stocks across the world, and generates a regularly refreshed stock universe and exports it in common formats for use in other programs.

## Outputs

Generated into `data/`:

- US universe:
  - `data/US_Stocks.csv`
  - `data/US_Stocks.sqlite` (table: `stocks`)
  - `data/US_Tickers.txt` (one ticker per line)
- Global universe:
  - `data/Global_Stocks.csv`
  - `data/Global_Stocks.sqlite` (table: `stocks`)
  - `data/Global_Tickers.txt` (one ticker per line)

## Data sources

- **Ticker universe**: NASDAQ Trader symbol directories
  - `nasdaqlisted.txt`
  - `otherlisted.txt`
- **Metadata enrichment**: `yfinance` (Yahoo Finance)

## Columns (canonical schema)

The generator normalizes a stable set of fields:

- `ticker`
- `company_name`
- `quote_type`
- `exchange`
- `exchange_name`
- `currency`
- `country`
- `sector`
- `industry`
- `website`
- `employees`
- `market_cap`
- `first_trade_date`
- `age_years_since_first_trade`
- `business_summary`
- `listing_source`
- `updated_at_utc`

Notes:
- `age_years_since_first_trade` is computed from Yahoo's `firstTradeDateEpochUtc` when available.
- Some tickers will have missing fields depending on what the provider returns.

## Run locally

```bash
python -m venv .venv
# activate venv
pip install -r requirements.txt
python src/build_stock_list.py --universe us
```

Optional flags:

- `--max-tickers N` (debug)
- `--universe us|global|all`
- `--workers N` (parallel yfinance requests; keep low to avoid rate limits)
- `--sleep-s S` (delay/backoff between requests)
- `--no-yfinance` (skip Yahoo enrichment; uses listing data only)
- `--start N` (start index into ticker list; for chunked runs)
- `--count N` (number of tickers to process from --start)
- `--db-mode replace|upsert` (replace = overwrite DB each run; upsert = persist across runs)
- `--verbose` (print stage-by-stage logs)

### Windows tip (no activation required)

You can run using the venv interpreter directly:

```powershell
\.\.venv\Scripts\python.exe src\build_stock_list.py --universe us --max-tickers 50
```

## GitHub Actions

A weekly workflow refreshes the outputs and commits updated files back into the repository.

## Progress output

The build shows progress bars when `tqdm` is installed (included in `requirements.txt`).

You can also pass `--verbose` to print stage-by-stage logs.

## Building a bigger global universe

Global tickers are sourced from `inputs/global_tickers.txt`.

To generate that file by merging multiple sources, use:

```powershell
\.\.venv\Scripts\python.exe src\build_global_universe.py
```

You can also provide an API-key provider (optional):

```powershell
$env:EODHD_API_KEY = "YOUR_KEY"
\.\.venv\Scripts\python.exe src\build_global_universe.py --eodhd-exchanges "US,TO,GER,SWX,TSE"
```

## Chunked enrichment (for large universes)

For the global universe (or very large US lists), Yahoo will rate-limit if you try to enrich everything in one run. Use chunked runs with `--db-mode upsert` to persist progress across runs:

```powershell
# Example: enrich 200 tickers at a time, persisting to the same DB/CSV
\.\.venv\Scripts\python.exe src\build_stock_list.py --universe global --global-source file --start 0 --count 200 --workers 1 --sleep-s 1.2 --db-mode upsert --verbose
\.\.venv\Scripts\python.exe src\build_stock_list.py --universe global --global-source file --start 200 --count 200 --workers 1 --sleep-s 1.2 --db-mode upsert --verbose
# Continue incrementing --start until you reach the end of the ticker list
```

- `--db-mode upsert` keeps the same SQLite DB and CSV, updating rows for the current chunk and adding new ones.
- Each run exports the full CSV from the DB, so you always get a complete file.
- Use `--workers 1` and `--sleep-s 1.2` (or higher) to avoid Yahoo rate limits.

## Credits

- NASDAQ Trader symbol directories: https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs
- yfinance: https://github.com/ranaroussi/yfinance

**Made with ❤️ by MeridianAlgo**