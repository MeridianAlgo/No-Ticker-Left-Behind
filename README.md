# TickerAtlas

**Every stock, ETF and crypto ticker in one open, machine-readable atlas.**

TickerAtlas (formerly *No-Ticker-Left-Behind*) is a daily-refreshed reference dataset of US-listed stocks and ETFs plus 12,000+ cryptocurrencies. It includes names, exchanges, sectors, industries, countries, market caps and company descriptions, published as CSV, JSON, SQLite and plain text. A static website and JSON API on GitHub Pages make every ticker easy to look up, for people and for AI agents.

- **Website & search:** https://meridianalgo.github.io/TickerAtlas/
- **API & AI docs:** https://meridianalgo.github.io/TickerAtlas/docs.html
- **LLM context:** [`llms.txt`](llms.txt) (repo) · https://meridianalgo.github.io/TickerAtlas/llms.txt (live, with counts)

## Quick start (no install)

```bash
# One ticker (stocks/ETFs under api/stocks/, crypto under api/crypto/)
curl https://meridianalgo.github.io/TickerAtlas/api/stocks/AAPL.json
curl https://meridianalgo.github.io/TickerAtlas/api/crypto/BTC.json

# Everything, as one flat CSV straight from this repo
curl -O https://raw.githubusercontent.com/MeridianAlgo/TickerAtlas/main/data/All_Assets.csv
```

```python
import pandas as pd
# keep_default_na=False stops pandas from reading the real ticker "NA" as missing
df = pd.read_csv("https://raw.githubusercontent.com/MeridianAlgo/TickerAtlas/main/data/All_Assets.csv",
                 keep_default_na=False, na_values=[""])
df[df.asset_class == "crypto"].head()
```

## Files in `data/`

| File | Contents |
| --- | --- |
| `All_Assets.csv` | **Start here.** Every stock, ETF and crypto: `id, ticker, name, asset_class, exchange, country, sector, source` |
| `All_Stocks.csv` | Full stock & ETF records (schema below) |
| `All_Stocks.sqlite` | Same, as SQLite (table `stocks`, indexed on ticker / exchange / sector / country) |
| `All_Tickers.txt` | Stock & ETF tickers, one per line |
| `Crypto.csv` | Crypto `ticker, name, asset_class, source` |
| `Crypto.json` | Crypto symbols + names with source metadata (upstream commit, license) |
| `Crypto_Tickers.txt` | Crypto symbols, one per line |
| `US_Stocks.*` | US-only intermediate build (same schema as `All_Stocks`) |
| `New_Tickers_Weekly.txt` | Stock tickers newly discovered in the latest run |

`id` (`stock:SOL`, `crypto:SOL`) is the unique key. `ticker` alone is **not** unique across asset classes.

### Stock & ETF schema (`All_Stocks.csv`)

| Column | Description |
| --- | --- |
| `ticker` | Symbol as published by NASDAQ Trader (class shares use a dot, e.g. `BRK.B`) |
| `company_name` | Company / fund name |
| `quote_type` | `EQUITY`, `ETF`, … (Yahoo quote type) |
| `exchange`, `exchange_name` | Exchange code and display name (e.g. `NMS` / `NasdaqGS`) |
| `currency`, `country` | Trading currency, company country |
| `sector`, `industry` | Yahoo Finance classification |
| `website`, `employees`, `market_cap` | Company website, full-time employees, market cap at update time |
| `first_trade_date`, `age_years_since_first_trade` | First trade date and age in years, when known |
| `business_summary` | Company description |
| `listing_source` | `nasdaqlisted`, `otherlisted` (NYSE, NYSE American, NYSE Arca, Cboe, IEX) or `file` (global) |
| `updated_at_utc` | When the row was last refreshed |

Some fields are empty when Yahoo Finance returns nothing for a symbol.

## Website & JSON API

`src/build_site.py` turns `data/` into a static site (deployed by `.github/workflows/deploy-site.yml`):

| Path | Returns |
| --- | --- |
| `api/stocks/{slug}.json`, `api/crypto/{slug}.json` | One record |
| `stocks/{slug}.html`, `crypto/{slug}.html` | Overview page with schema.org JSON-LD |
| `api/index.json` | Compact columnar index of every asset (powers the search box) |
| `api/stocks.json`, `api/crypto.json`, `api/meta.json` | Bulk records and build metadata |
| `llms.txt`, `llms-full.txt`, `openapi.json`, `sitemap.xml` | AI / crawler-friendly descriptions |
| `data/*` | Copies of the raw downloads |

`{slug}` is the ticker with every character outside `[A-Za-z0-9.-]` replaced by `_` + its UTF-8 hex (`$MAID` → `_24MAID`).

## Run locally

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python src/build_stock_list.py --universe us --db-mode upsert --verbose   # US listings + Yahoo metadata
python src/build_stock_list.py --universe global --global-source file      # optional: inputs/global_tickers.txt
python src/combine_universes.py                                            # -> data/All_Stocks.*
python src/build_crypto_list.py                                            # -> data/Crypto.*
python src/build_asset_index.py                                            # -> data/All_Assets.csv
python src/build_site.py --out _site                                       # -> static site
python -m http.server 8000 --directory _site                               # preview at http://localhost:8000
```

Useful `build_stock_list.py` flags: `--max-tickers N` (debug), `--no-yfinance` (listing data only), `--workers N`, `--sleep-s S`, `--start N --count N` (chunked enrichment), `--db-mode replace|upsert`.

### Bigger global universe

Global tickers (Yahoo symbols such as `VOD.L`) are read from `inputs/global_tickers.txt`, which `src/build_global_universe.py` generates. It can optionally pull exchange symbol lists from EODHD:

```bash
EODHD_API_KEY=... python src/build_global_universe.py --eodhd-exchanges "US,TO,GER,SWX,TSE"
```

Yahoo rate-limits large runs. Use chunked runs (`--start/--count`) with `--db-mode upsert`, `--workers 1` and `--sleep-s 1.2`.

## Automation

- **`update-stock-universe.yml`** (daily, 02:00 UTC): refreshes listings, enriches with Yahoo Finance, rebuilds crypto and the unified index, writes `New_Tickers_Weekly.txt`, and commits `data/`. It opens a GitHub issue on failure and closes it on the next success.
- **`deploy-site.yml`**: rebuilds and deploys the website + API to GitHub Pages whenever `data/` or the site code changes on `main`.

## Sources & credits

TickerAtlas compiles data from the sources below. Please credit them when you use it. Details are in [SOURCES.md](SOURCES.md).

- **US listings:** [NASDAQ Trader symbol directory](https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs) (`nasdaqlisted.txt`, `otherlisted.txt`)
- **Company metadata:** [Yahoo Finance](https://finance.yahoo.com/) via [yfinance](https://github.com/ranaroussi/yfinance)
- **Cryptocurrencies:** [crypti/cryptocurrencies](https://github.com/crypti/cryptocurrencies) (MIT, © Michael Wuergler), generated from the [CryptoCompare](https://www.cryptocompare.com/) coin list

TickerAtlas is reference data, not investment advice. Fields may be incomplete or out of date.

**Made with ❤️ by [MeridianAlgo](https://meridianalgo.org/)**
