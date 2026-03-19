# NTLB (No-Ticker-Left-Behind)

This repository contians all tickers for all stocks across the world, and generates a regularly refreshed stock universe and exports it in common formats for use in other programs.

## Outputs

Generated into `data/`:

- **All_Stocks.csv** - Combined universe with all stocks and detailed company information
- **All_Stocks.sqlite** - Indexed database (table: `stocks`) for fast queries
- **All_Tickers.txt** - Simple ticker list (one per line)

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

# Build US universe
python src/build_stock_list.py --universe us

# Build Global universe
python src/build_stock_list.py --universe global --global-source file

# Combine both universes into a single comprehensive list
python src/combine_universes.py --out-csv data/All_Stocks.csv --out-db data/All_Stocks.sqlite --out-txt data/All_Tickers.txt --out-json data/All_Stocks.json
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

A weekly workflow (every Monday at 2 AM UTC) automatically:
- Refreshes `inputs/global_tickers.txt` to discover newly listed symbols
- Fetches the latest stock data from free APIs (Yahoo Finance, NASDAQ Trader)
- Builds US and Global universes in upsert mode so new tickers are added to the database
- Combines them into a single comprehensive dataset
- Writes `data/New_Tickers_Weekly.txt` with only the symbols discovered in that run
- Commits updated files back to the repository
- Creates GitHub issues if the build fails
- Auto-closes issues when the build succeeds

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

## Combining universes

After building both US and Global universes, combine them into a single comprehensive dataset:

```bash
python src/combine_universes.py --out-csv data/All_Stocks.csv --out-db data/All_Stocks.sqlite --out-txt data/All_Tickers.txt
```

This creates:
- `data/All_Stocks.csv` - All stocks with detailed company information
- `data/All_Stocks.sqlite` - Indexed database for fast queries
- `data/All_Tickers.txt` - All unique tickers

The combined dataset includes:
- **Detailed company information**: name, sector, industry, country, exchange, market cap, employees, website, business summary
- **Indexed queries**: Fast lookups by ticker, exchange, sector, or country
- **Deduplication**: Automatic removal of duplicate tickers across universes

## Enrichment Status

The enrichment process fetches detailed company information from Yahoo Finance for each ticker. This takes time due to rate limits:

- **US Stocks**: ✅ Fully enriched (5,263 stocks)
- **Global Stocks**: ⚠️ In progress (57,358 stocks)
- **Estimated time**: ~27 hours for full global enrichment at 1.7s per ticker

### Check enrichment progress:
```bash
python check_enrichment_status.py
```

### Continue enrichment:
```bash
# Process all remaining stocks automatically
python enrich_all_global_stocks.py

# Or process individual chunks manually (500 tickers each)
python enrich_global_stocks.py 1000  # Process tickers 1000-1500
python enrich_global_stocks.py 1500  # Process tickers 1500-2000
# Continue incrementing by 500...
```

The enrichment uses:
- **Chunked processing**: 500 tickers at a time
- **Database upsert mode**: Progress is saved after each chunk
- **Rate limit protection**: 1.5s delay between requests, automatic retry with exponential backoff
- **Single worker**: Avoids overwhelming Yahoo Finance API

## Credits

- NASDAQ Trader symbol directories: https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs
- yfinance: https://github.com/ranaroussi/yfinance

**Made with ❤️ by MeridianAlgo**