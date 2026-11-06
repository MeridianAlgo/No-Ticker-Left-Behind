"""
Generate the TickerAtlas static website + JSON API (deployed to GitHub Pages).

    python src/build_site.py --out _site --site-url https://meridianalgo.github.io/TickerAtlas/

Inputs (all in data/): All_Stocks.csv, Crypto.csv, All_Assets.csv (+ other files copied as downloads)
Static assets: site/assets/*

Output layout (everything is plain static files, CORS-enabled on GitHub Pages):

    index.html                 search UI over every ticker
    docs.html                  API & AI-agent documentation
    sources.html               data sources, licenses, disclaimers
    stocks/<slug>.html         one overview page per stock / ETF
    crypto/<slug>.html         one overview page per cryptocurrency
    api/index.json             compact search index of all assets (columnar rows)
    api/meta.json              counts, build time, sources
    api/stocks.json            every stock/ETF record
    api/crypto.json            every crypto record
    api/stocks/<slug>.json     one stock/ETF record
    api/crypto/<slug>.json     one crypto record
    openapi.json               OpenAPI 3.1 description of the endpoints above
    llms.txt / llms-full.txt   LLM-oriented summary + full ticker listing
    sitemap.xml, robots.txt, .nojekyll, 404.html
    data/*                     raw downloads (CSV, JSON, SQLite, TXT)

<slug> is the ticker with every character outside [A-Za-z0-9.-] replaced by "_" +
its UTF-8 bytes in uppercase hex. E.g. AAPL -> AAPL, BRK.B -> BRK.B, $MAID -> _24MAID.
"""

import argparse
import datetime as dt
import html
import json
import math
import os
import re
import shutil
import string
import sys
from collections import Counter
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO_URL = "https://github.com/MeridianAlgo/TickerAtlas"
RAW_URL = "https://raw.githubusercontent.com/MeridianAlgo/TickerAtlas/main"
DEFAULT_SITE_URL = "https://meridianalgo.github.io/TickerAtlas/"

SAFE_CHARS = set(string.ascii_letters + string.digits + ".-")

DOWNLOADS = [
    ("All_Assets.csv", "Unified index: every stock, ETF and crypto (id, ticker, name, asset_class, exchange, country, sector, source)"),
    ("All_Stocks.csv", "Full stock & ETF records with company metadata"),
    ("All_Stocks.sqlite", "SQLite database, table <code>stocks</code>, indexed on ticker / exchange / sector / country"),
    ("All_Tickers.txt", "Stock & ETF tickers, one per line"),
    ("Crypto.csv", "Cryptocurrency symbols and names"),
    ("Crypto.json", "Cryptocurrency symbols and names with source metadata"),
    ("Crypto_Tickers.txt", "Crypto symbols, one per line"),
    ("New_Tickers_Weekly.txt", "Stock tickers newly discovered in the latest run"),
]

SOURCES = [
    {
        "name": "NASDAQ Trader Symbol Directory",
        "url": "https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs",
        "provides": "The US listing universe: every security on Nasdaq (nasdaqlisted.txt) and on NYSE, NYSE American, NYSE Arca, Cboe and IEX (otherlisted.txt).",
        "license": "Public symbol directory published by Nasdaq, Inc.",
    },
    {
        "name": "Yahoo Finance (via yfinance)",
        "url": "https://finance.yahoo.com/",
        "provides": "Company metadata: names, sector, industry, country, website, employees, market cap, business summary.",
        "license": "Retrieved with the open-source yfinance library (https://github.com/ranaroussi/yfinance, Apache-2.0). Yahoo Finance data is subject to Yahoo's terms of service and is intended for personal and research use.",
    },
    {
        "name": "crypti/cryptocurrencies",
        "url": "https://github.com/crypti/cryptocurrencies",
        "provides": "The cryptocurrency universe: symbols and names.",
        "license": "MIT License, Copyright (c) Michael Wuergler. Upstream data is generated from the CryptoCompare coin list.",
    },
    {
        "name": "CryptoCompare",
        "url": "https://www.cryptocompare.com/",
        "provides": "Original source of the coin list that crypti/cryptocurrencies is built from.",
        "license": "Referenced for attribution; TickerAtlas does not query CryptoCompare directly.",
    },
]

CRYPTI_MIT = """The MIT License (MIT)

Copyright (c) Michael Wuergler <wuergler@gmail.com> (numetriclabs.com)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE."""

LOGO_SVG = (
    '<svg viewBox="0 0 32 32" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">'
    '<circle cx="16" cy="16" r="13"/><ellipse cx="16" cy="16" rx="5.5" ry="13"/>'
    '<path d="M3.5 12h25M3.5 20h25"/></svg>'
)
SEARCH_SVG = (
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" aria-hidden="true">'
    '<circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>'
)


# --------------------------------------------------------------------------- helpers


def slugify(ticker: str) -> str:
    out = []
    for ch in ticker:
        if ch in SAFE_CHARS:
            out.append(ch)
        else:
            out.append("".join(f"_{b:02X}" for b in ch.encode("utf-8")))
    s = "".join(out)
    if not s.strip("."):  # ".", ".." etc. are not valid file names
        s = "".join(f"_{b:02X}" for b in ticker.encode("utf-8"))
    return s


def e(x: Any) -> str:
    return html.escape("" if x is None else str(x), quote=True)


def clean(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, str):
        v = v.strip()
        return v or None
    return v


def to_int(v: Any) -> Optional[int]:
    v = clean(v)
    if v is None:
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def to_float(v: Any) -> Optional[float]:
    v = clean(v)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def fmt_money(v: Optional[int], currency: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    sym = "$" if (currency or "USD") == "USD" else ""
    suffix = "" if sym else f" {currency}"
    for div, unit in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(v) >= div:
            return f"{sym}{v / div:,.2f}{unit}{suffix}"
    return f"{sym}{v:,}{suffix}"


def to_yahoo_symbol(ticker: str, listing_source: Optional[str]) -> str:
    """Same rule as build_stock_list.to_yahoo_symbol; applied to US listings only."""
    if listing_source not in ("nasdaqlisted", "otherlisted"):
        return ticker
    t = ticker.replace("$", "-P")
    if re.fullmatch(r"[A-Z]+\.[A-Z]", t):
        t = t.replace(".", "-")
    return t


def json_ld(obj: Dict[str, Any]) -> str:
    s = json.dumps(obj, ensure_ascii=False)
    return s.replace("</", "<\\/")


def write(path: str, content: str) -> bool:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(content)
        return True
    except OSError as ex:  # e.g. reserved names like CON.html on Windows
        print(f"Warning: could not write {path}: {ex}", file=sys.stderr)
        return False


def write_json(path: str, obj: Any, compact: bool = True) -> bool:
    if compact:
        s = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    else:
        s = json.dumps(obj, ensure_ascii=False, indent=2)
    return write(path, s + "\n")


def read_csv_safe(path: str) -> pd.DataFrame:
    return pd.read_csv(path, keep_default_na=False, na_values=[""], dtype=str)


# --------------------------------------------------------------------------- data


def load_stock_records(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    df = read_csv_safe(path)
    recs: List[Dict[str, Any]] = []
    seen = set()
    for row in df.to_dict(orient="records"):
        t = clean(row.get("ticker"))
        if not t or t in seen:
            continue
        seen.add(t)
        qt = clean(row.get("quote_type"))
        ac = {"EQUITY": "stock", "ETF": "etf", "MUTUALFUND": "fund", "INDEX": "index"}.get(
            (qt or "").upper(), (qt or "stock").lower()
        )
        slug = slugify(t)
        src = clean(row.get("listing_source"))
        rec = {
            "id": f"{ac}:{t}",
            "ticker": t,
            "name": clean(row.get("company_name")) or t,
            "asset_class": ac,
            "quote_type": qt,
            "exchange": clean(row.get("exchange")),
            "exchange_name": clean(row.get("exchange_name")),
            "currency": clean(row.get("currency")),
            "country": clean(row.get("country")),
            "sector": clean(row.get("sector")),
            "industry": clean(row.get("industry")),
            "website": clean(row.get("website")),
            "employees": to_int(row.get("employees")),
            "market_cap": to_int(row.get("market_cap")),
            "first_trade_date": clean(row.get("first_trade_date")),
            "age_years_since_first_trade": to_float(row.get("age_years_since_first_trade")),
            "business_summary": clean(row.get("business_summary")),
            "listing_source": src,
            "updated_at_utc": clean(row.get("updated_at_utc")),
            "yahoo_symbol": to_yahoo_symbol(t, src),
            "slug": slug,
            "page": f"stocks/{slug}.html",
            "json": f"api/stocks/{slug}.json",
        }
        recs.append(rec)
    recs.sort(key=lambda r: (r["ticker"].upper(), r["ticker"]))
    return recs


def load_crypto_records(path: str, source_commit: Optional[str]) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    df = read_csv_safe(path)
    recs = []
    seen = set()
    for row in df.to_dict(orient="records"):
        t = clean(row.get("ticker"))
        if not t or t in seen:
            continue
        seen.add(t)
        slug = slugify(t)
        recs.append(
            {
                "id": f"crypto:{t}",
                "ticker": t,
                "name": clean(row.get("name")) or t,
                "asset_class": "crypto",
                "source": "https://github.com/crypti/cryptocurrencies",
                "source_commit": source_commit,
                "source_license": "MIT",
                "original_source": "https://www.cryptocompare.com/",
                "slug": slug,
                "page": f"crypto/{slug}.html",
                "json": f"api/crypto/{slug}.json",
            }
        )
    recs.sort(key=lambda r: (r["ticker"].upper(), r["ticker"]))
    return recs


def check_case_collisions(recs: List[Dict[str, Any]], label: str) -> None:
    c = Counter(r["slug"].lower() for r in recs)
    dups = [s for s, n in c.items() if n > 1]
    if dups:
        print(
            f"Warning: {len(dups)} {label} slugs differ only by case (fine on GitHub Pages, "
            f"but not on case-insensitive file systems): {dups[:10]}",
            file=sys.stderr,
        )


# --------------------------------------------------------------------------- layout


def header(prefix: str, current: str = "") -> str:
    def nav(href: str, label: str, key: str) -> str:
        cur = ' aria-current="page"' if key == current else ""
        return f'<a href="{prefix}{href}"{cur}>{label}</a>'

    return (
        '<header class="site-header"><div class="wrap">'
        f'<a class="brand" href="{prefix}index.html">{LOGO_SVG}<span>TickerAtlas</span></a>'
        '<nav class="nav" aria-label="Main">'
        + nav("index.html", "Search", "search")
        + nav("docs.html", "API &amp; AI", "docs")
        + nav("sources.html", "Sources", "sources")
        + f'<a href="{REPO_URL}">GitHub</a>'
        + "</nav></div></header>"
    )


def footer(prefix: str, built: str) -> str:
    return (
        '<footer class="site-footer"><div class="wrap">'
        "<p><strong>Sources:</strong> US listings from the "
        '<a href="https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs">NASDAQ Trader symbol directory</a>; '
        'company metadata from <a href="https://finance.yahoo.com/">Yahoo Finance</a> via '
        '<a href="https://github.com/ranaroussi/yfinance">yfinance</a>; cryptocurrencies from '
        '<a href="https://github.com/crypti/cryptocurrencies">crypti/cryptocurrencies</a> (MIT, &copy; Michael Wuergler), '
        'originally from the <a href="https://www.cryptocompare.com/">CryptoCompare</a> coin list. '
        f'<a href="{prefix}sources.html">Full attribution</a>.</p>'
        "<p>Reference data only &mdash; not investment advice. Fields can be missing or out of date. "
        f"Built {e(built)} from <a href=\"{REPO_URL}\">MeridianAlgo/TickerAtlas</a>.</p>"
        "</div></footer>"
    )


def page(
    *,
    title: str,
    description: str,
    prefix: str,
    canonical: str,
    body: str,
    built: str,
    current: str = "",
    ld: Optional[Dict[str, Any]] = None,
    alt_json: Optional[str] = None,
    scripts: str = "",
) -> str:
    alt = f'<link rel="alternate" type="application/json" href="{e(alt_json)}">' if alt_json else ""
    ld_tag = f'<script type="application/ld+json">{json_ld(ld)}</script>' if ld else ""
    return (
        "<!doctype html>\n"
        '<html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f"<title>{e(title)}</title>"
        f'<meta name="description" content="{e(description)}">'
        f'<link rel="canonical" href="{e(canonical)}">'
        f'<meta property="og:title" content="{e(title)}">'
        f'<meta property="og:description" content="{e(description)}">'
        '<meta property="og:type" content="website">'
        f"{alt}"
        f'<link rel="stylesheet" href="{prefix}assets/style.css">'
        f'<link rel="icon" href="{prefix}assets/favicon.svg" type="image/svg+xml">'
        f"{ld_tag}</head><body>"
        f"{header(prefix, current)}<main class=\"wrap\">{body}</main>{footer(prefix, built)}"
        f"{scripts}</body></html>\n"
    )


def badge(ac: str) -> str:
    cls = ac if ac in ("stock", "etf", "crypto") else "other"
    return f'<span class="badge {cls}">{e(ac)}</span>'


def facts(items: List[tuple]) -> str:
    cells = "".join(
        f'<div class="fact"><dt>{e(k)}</dt><dd>{v}</dd></div>' for k, v in items if v not in (None, "")
    )
    return f'<dl class="facts">{cells}</dl>' if cells else ""


# --------------------------------------------------------------------------- detail pages


def stock_page(r: Dict[str, Any], site_url: str, built: str, crypto_twin: Optional[Dict[str, Any]]) -> str:
    t, name, ac = r["ticker"], r["name"], r["asset_class"]
    kind = {"stock": "stock", "etf": "ETF", "fund": "fund"}.get(ac, ac)
    exch = r["exchange_name"] or r["exchange"]
    website = r["website"]
    items = [
        ("Ticker", f'<span class="mono">{e(t)}</span>'),
        ("Type", e(kind)),
        ("Exchange", e(exch) if exch else None),
        ("Currency", e(r["currency"]) if r["currency"] else None),
        ("Country", e(r["country"]) if r["country"] else None),
        ("Sector", e(r["sector"]) if r["sector"] else None),
        ("Industry", e(r["industry"]) if r["industry"] else None),
        ("Market cap", e(fmt_money(r["market_cap"], r["currency"])) if r["market_cap"] else None),
        ("Employees", f'{r["employees"]:,}' if r["employees"] else None),
        ("First trade", e(r["first_trade_date"]) if r["first_trade_date"] else None),
        (
            "Website",
            f'<a href="{e(website)}" rel="nofollow noopener">{e(re.sub(r"^https?://(www\.)?", "", website).rstrip("/"))}</a>'
            if website
            else None,
        ),
        ("Listing source", e(r["listing_source"]) if r["listing_source"] else None),
    ]
    summary = f'<p class="summary">{e(r["business_summary"])}</p>' if r["business_summary"] else ""
    links = [
        f'<a class="btn primary" href="../{e(r["json"])}">JSON record</a>',
        f'<a class="btn" href="https://finance.yahoo.com/quote/{quote(r["yahoo_symbol"])}" rel="nofollow noopener">Yahoo Finance</a>',
    ]
    if r["listing_source"] in ("nasdaqlisted", "otherlisted") and ac == "stock":
        links.append(
            f'<a class="btn" href="https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&amp;CIK={quote(t)}" rel="nofollow noopener">SEC filings</a>'
        )
    twin = ""
    if crypto_twin:
        twin = (
            f'<p class="note">The symbol <span class="mono">{e(t)}</span> is also used by a cryptocurrency: '
            f'<a href="../{e(crypto_twin["page"])}">{e(crypto_twin["name"])}</a>.</p>'
        )
    updated = f'<p class="crumbs">Record updated {e(r["updated_at_utc"])}</p>' if r["updated_at_utc"] else ""
    body = (
        f'<p class="crumbs"><a href="../index.html">TickerAtlas</a> / <a href="../index.html?type={"etf" if ac == "etf" else "stock"}">{"ETFs" if ac == "etf" else "Stocks"}</a> / {e(t)}</p>'
        f'<div class="ticker-head"><div><h1 class="ticker-sym">{e(t)}</h1><p class="ticker-name">{e(name)}</p></div>{badge(ac)}</div>'
        f"{facts(items)}{summary}{twin}"
        f'<div class="links">{"".join(links)}</div>{updated}'
    )
    desc_bits = [f"{name} ({t})", f"{kind} listed on {exch}" if exch else kind]
    if r["sector"]:
        desc_bits.append(f"sector: {r['sector']}")
    if r["industry"]:
        desc_bits.append(f"industry: {r['industry']}")
    if r["country"]:
        desc_bits.append(f"country: {r['country']}")
    description = ", ".join(desc_bits) + ". Ticker overview and machine-readable JSON from TickerAtlas."
    ld: Dict[str, Any] = {
        "@context": "https://schema.org",
        "@type": "InvestmentFund" if ac == "etf" else "Corporation",
        "name": name,
        "tickerSymbol": f"{exch} {t}" if exch else t,
        "url": site_url + r["page"],
    }
    if website:
        ld["sameAs"] = website
    if r["business_summary"]:
        ld["description"] = r["business_summary"][:500]
    if r["employees"]:
        ld["numberOfEmployees"] = {"@type": "QuantitativeValue", "value": r["employees"]}
    return page(
        title=f"{t} — {name} | TickerAtlas",
        description=description,
        prefix="../",
        canonical=site_url + r["page"],
        body=body,
        built=built,
        ld=ld,
        alt_json="../" + r["json"],
    )


def crypto_page(r: Dict[str, Any], site_url: str, built: str, stock_twin: Optional[Dict[str, Any]]) -> str:
    t, name = r["ticker"], r["name"]
    items = [
        ("Symbol", f'<span class="mono">{e(t)}</span>'),
        ("Name", e(name)),
        ("Asset class", "Cryptocurrency"),
        ("Source", '<a href="https://github.com/crypti/cryptocurrencies">crypti/cryptocurrencies</a> (MIT)'),
    ]
    twin = ""
    if stock_twin:
        twin = (
            f'<p class="note">The symbol <span class="mono">{e(t)}</span> is also a listed security: '
            f'<a href="../{e(stock_twin["page"])}">{e(stock_twin["name"])}</a>. '
            "Always disambiguate by asset class.</p>"
        )
    body = (
        f'<p class="crumbs"><a href="../index.html">TickerAtlas</a> / <a href="../index.html?type=crypto">Crypto</a> / {e(t)}</p>'
        f'<div class="ticker-head"><div><h1 class="ticker-sym">{e(t)}</h1><p class="ticker-name">{e(name)}</p></div>{badge("crypto")}</div>'
        f"{facts(items)}{twin}"
        f'<div class="links"><a class="btn primary" href="../{e(r["json"])}">JSON record</a>'
        f'<a class="btn" href="https://www.cryptocompare.com/coins/{quote(t.lower())}/overview" rel="nofollow noopener">CryptoCompare</a></div>'
        '<p class="crumbs">Symbol list from crypti/cryptocurrencies, generated from the CryptoCompare coin list. '
        "Crypto symbols are not standardized: several projects can share a symbol.</p>"
    )
    ld = {
        "@context": "https://schema.org",
        "@type": "FinancialProduct",
        "category": "Cryptocurrency",
        "name": name,
        "alternateName": t,
        "url": site_url + r["page"],
    }
    return page(
        title=f"{t} — {name} (crypto) | TickerAtlas",
        description=f"{name} ({t}) cryptocurrency symbol. Overview and machine-readable JSON from TickerAtlas.",
        prefix="../",
        canonical=site_url + r["page"],
        body=body,
        built=built,
        ld=ld,
        alt_json="../" + r["json"],
    )


# --------------------------------------------------------------------------- top-level pages


def index_page(stats: Dict[str, Any], site_url: str, built: str) -> str:
    n = stats["counts"]
    stat_html = "".join(
        f'<div class="stat"><b>{v}</b><span>{k}</span></div>'
        for k, v in [
            ("Total assets", f"{n['total']:,}"),
            ("Stocks", f"{n['stock']:,}"),
            ("ETFs", f"{n['etf']:,}"),
            ("Cryptocurrencies", f"{n['crypto']:,}"),
        ]
    )
    downloads = "".join(
        f'<tr><td><a href="data/{f}"><code>{f}</code></a></td><td>{d}</td></tr>' for f, d in DOWNLOADS
    )
    body = f"""
<section class="hero">
  <p class="eyebrow">Open ticker reference · updated daily</p>
  <h1>Every ticker, one atlas.</h1>
  <p class="lede">{n['total']:,} stocks, ETFs and cryptocurrencies with names, exchanges, sectors and company
  profiles. Search them here, or point an AI agent at the static JSON API. No keys, no rate limits.</p>
  <div class="stats">{stat_html}</div>
</section>

<section class="search-panel" aria-label="Search tickers">
  <div class="search-row">
    <label class="search-box">{SEARCH_SVG}<span class="sr" hidden>Search</span>
      <input id="q" type="search" autocomplete="off" spellcheck="false" placeholder="Search by ticker or name: AAPL, Vanguard, Bitcoin…" aria-describedby="result-meta" autofocus>
    </label>
    <div class="chips" role="group" aria-label="Filter by type">
      <button class="chip" data-type="all" aria-pressed="true">All</button>
      <button class="chip" data-type="stock" aria-pressed="false">Stocks</button>
      <button class="chip" data-type="etf" aria-pressed="false">ETFs</button>
      <button class="chip" data-type="crypto" aria-pressed="false">Crypto</button>
    </div>
  </div>
  <p class="result-meta" id="result-meta" aria-live="polite"></p>
  <ul class="results" id="results"></ul>
  <noscript><p>Search needs JavaScript. Every ticker also has a static page, listed in <a href="sitemap.xml">sitemap.xml</a> and <a href="llms-full.txt">llms-full.txt</a>.</p></noscript>
</section>

<section class="block">
  <h2>Built for AI agents</h2>
  <p>Every ticker has a stable, predictable URL as both an HTML page and a JSON record. Everything is a static file with
  permissive CORS, so agents, notebooks and browsers can fetch it directly. Full reference: <a href="docs.html">API &amp; AI docs</a>.</p>
  <div class="grid-2">
    <div class="card"><h3>Look up one ticker</h3><p>Stocks and ETFs live under <code>api/stocks/</code>, crypto under <code>api/crypto/</code>.</p>
<pre><code>curl {e(site_url)}api/stocks/AAPL.json
curl {e(site_url)}api/crypto/BTC.json</code></pre></div>
    <div class="card"><h3>Get every ticker at once</h3><p>A compact index of all {n['total']:,} assets, or the full CSV.</p>
<pre><code>curl {e(site_url)}api/index.json
curl {e(site_url)}data/All_Assets.csv</code></pre></div>
    <div class="card"><h3>LLM-ready context</h3><p><a href="llms.txt"><code>llms.txt</code></a> explains the dataset in plain text; <a href="llms-full.txt"><code>llms-full.txt</code></a> lists every ticker and name, one per line.</p></div>
    <div class="card"><h3>Tool &amp; plugin specs</h3><p><a href="openapi.json"><code>openapi.json</code></a> describes every endpoint, so you can register TickerAtlas as a tool for an agent framework in one step.</p></div>
  </div>
</section>

<section class="block">
  <h2>Downloads</h2>
  <p>Raw files, regenerated daily by GitHub Actions. Also available straight from the repository at <code>{e(RAW_URL)}/data/&lt;file&gt;</code>.</p>
  <div class="table-scroll"><table class="tbl"><thead><tr><th>File</th><th>Contents</th></tr></thead><tbody>{downloads}</tbody></table></div>
</section>
"""
    ld = {
        "@context": "https://schema.org",
        "@type": "Dataset",
        "name": "TickerAtlas",
        "description": "Open reference dataset of stock, ETF and cryptocurrency tickers with names, exchanges, sectors and company profiles.",
        "url": site_url,
        "isAccessibleForFree": True,
        "creator": {"@type": "Organization", "name": "MeridianAlgo", "url": "https://meridianalgo.org/"},
        "dateModified": stats["generated_at"],
        "keywords": ["stock tickers", "ETF", "cryptocurrency symbols", "ticker list", "securities reference data"],
        "isBasedOn": [s["url"] for s in SOURCES],
        "distribution": [
            {"@type": "DataDownload", "encodingFormat": "text/csv", "contentUrl": site_url + "data/All_Assets.csv"},
            {"@type": "DataDownload", "encodingFormat": "application/json", "contentUrl": site_url + "api/index.json"},
            {"@type": "DataDownload", "encodingFormat": "application/vnd.sqlite3", "contentUrl": site_url + "data/All_Stocks.sqlite"},
        ],
    }
    return page(
        title="TickerAtlas — every stock, ETF and crypto ticker",
        description=f"Search {n['total']:,} stock, ETF and cryptocurrency tickers. Free static JSON API and downloads built for AI agents.",
        prefix="",
        canonical=site_url,
        body=body,
        built=built,
        current="search",
        ld=ld,
        alt_json="api/index.json",
        scripts='<script src="assets/app.js" defer></script>',
    )


STOCK_FIELDS = [
    ("id", "string", "Unique key: <code>&lt;asset_class&gt;:&lt;ticker&gt;</code>, e.g. <code>stock:AAPL</code>"),
    ("ticker", "string", "Exchange symbol as published by NASDAQ Trader (class shares use a dot, e.g. <code>BRK.B</code>)"),
    ("name", "string", "Company or fund name"),
    ("asset_class", "string", "<code>stock</code>, <code>etf</code>, or another lower-cased Yahoo quote type"),
    ("quote_type", "string|null", "Raw Yahoo quote type (EQUITY, ETF, …)"),
    ("exchange / exchange_name", "string|null", "Exchange code and display name (e.g. NMS / NasdaqGS)"),
    ("currency, country", "string|null", "Trading currency and company country"),
    ("sector, industry", "string|null", "Yahoo Finance classification"),
    ("website", "string|null", "Company website"),
    ("employees", "integer|null", "Full-time employees"),
    ("market_cap", "integer|null", "Market capitalization in <code>currency</code> at the time of the update"),
    ("first_trade_date", "string|null", "ISO date of the first trade, when known"),
    ("business_summary", "string|null", "Company description"),
    ("listing_source", "string", "<code>nasdaqlisted</code>, <code>otherlisted</code> or <code>file</code>"),
    ("updated_at_utc", "string", "When this record was last refreshed"),
    ("yahoo_symbol", "string", "Symbol to use with Yahoo Finance / yfinance (e.g. <code>BRK-B</code>)"),
    ("slug, page, json", "string", "File-safe slug and relative URLs of this record's HTML page and JSON"),
]


def docs_page(stats: Dict[str, Any], site_url: str, built: str) -> str:
    rows = "".join(f"<tr><td><code>{k}</code></td><td>{t}</td><td>{d}</td></tr>" for k, t, d in STOCK_FIELDS)
    endpoints = [
        ("api/index.json", "Every asset as columnar rows: <code>fields</code> + <code>rows</code>. Best for search and autocomplete."),
        ("api/stocks/{slug}.json", "One stock or ETF record (all fields)."),
        ("api/crypto/{slug}.json", "One cryptocurrency record."),
        ("api/stocks.json", "All stock &amp; ETF records in one array."),
        ("api/crypto.json", "All crypto records in one array."),
        ("api/meta.json", "Counts, build time and sources."),
        ("stocks/{slug}.html, crypto/{slug}.html", "Human-readable overview pages with schema.org JSON-LD."),
        ("data/All_Assets.csv", "Unified flat CSV of every asset."),
        ("llms.txt, llms-full.txt", "Plain-text context for language models."),
        ("openapi.json", "OpenAPI 3.1 spec of these endpoints."),
    ]
    ep_rows = "".join(f'<tr><td><code>{e(p)}</code></td><td>{d}</td></tr>' for p, d in endpoints)
    body = f"""
<section class="hero">
  <p class="eyebrow">Reference</p>
  <h1>API &amp; AI access</h1>
  <p class="lede">TickerAtlas is a static API: plain JSON, CSV and text files served from GitHub Pages. There are no keys, no rate limits
  beyond GitHub's, and CORS is open, so you can call it from a browser, a notebook or an agent.</p>
</section>

<section class="block">
  <h2>Base URL</h2>
  <pre><code>{e(site_url)}</code></pre>
  <p>All paths below are relative to this base URL.</p>
</section>

<section class="block">
  <h2>Endpoints</h2>
  <div class="table-scroll"><table class="tbl"><thead><tr><th>Path</th><th>Returns</th></tr></thead><tbody>{ep_rows}</tbody></table></div>
</section>

<section class="block">
  <h2>Building a URL from a ticker</h2>
  <p>The <code>{{slug}}</code> is the ticker itself when it only contains letters, digits, <code>.</code> and <code>-</code>, which covers
  almost every stock and coin. Any other character becomes <code>_</code> followed by its UTF-8 bytes in uppercase hex.
  Slugs are case-sensitive.</p>
  <div class="table-scroll"><table class="tbl"><thead><tr><th>Ticker</th><th>Slug</th><th>URL</th></tr></thead><tbody>
    <tr><td><code>AAPL</code></td><td><code>AAPL</code></td><td><code>api/stocks/AAPL.json</code></td></tr>
    <tr><td><code>BRK.B</code></td><td><code>BRK.B</code></td><td><code>api/stocks/BRK.B.json</code></td></tr>
    <tr><td><code>ABR$D</code></td><td><code>ABR_24D</code></td><td><code>api/stocks/ABR_24D.json</code></td></tr>
    <tr><td><code>BTC</code></td><td><code>BTC</code></td><td><code>api/crypto/BTC.json</code></td></tr>
    <tr><td><code>$MAID</code></td><td><code>_24MAID</code></td><td><code>api/crypto/_24MAID.json</code></td></tr>
  </tbody></table></div>
<pre><code># Python
def slug(ticker):
    safe = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789.-")
    return "".join(c if c in safe else "".join(f"_{{b:02X}}" for b in c.encode()) for c in ticker)</code></pre>
  <p class="note">A symbol can exist in more than one asset class: <code>SOL</code> is an NYSE-listed company <em>and</em> Solana. Check
  both <code>api/stocks/</code> and <code>api/crypto/</code>, or use the <code>id</code> field (<code>stock:SOL</code> vs <code>crypto:SOL</code>) as the unique key.</p>
</section>

<section class="block">
  <h2>Examples</h2>
  <div class="grid-2">
    <div class="card"><h3>curl + jq</h3>
<pre><code>curl -s {e(site_url)}api/stocks/MSFT.json | jq '{{name, sector, market_cap}}'</code></pre></div>
    <div class="card"><h3>Python</h3>
<pre><code>import requests
idx = requests.get("{e(site_url)}api/index.json").json()
rows = [dict(zip(idx["fields"], r)) for r in idx["rows"]]
etfs = [r for r in rows if r["asset_class"] == "etf"]</code></pre></div>
    <div class="card"><h3>pandas</h3>
<pre><code>import pandas as pd
df = pd.read_csv("{e(site_url)}data/All_Assets.csv",
                 keep_default_na=False, na_values=[""])</code></pre>
<p>Pass <code>keep_default_na=False</code>, or pandas will read the real ticker <code>NA</code> as a missing value.</p></div>
    <div class="card"><h3>JavaScript</h3>
<pre><code>const r = await fetch("{e(site_url)}api/crypto/ETH.json");
const {{ name }} = await r.json(); // "Ethereum"</code></pre></div>
  </div>
</section>

<section class="block">
  <h2>Instructions for AI agents</h2>
  <ol>
    <li>To resolve a single ticker, fetch <code>api/stocks/{{slug}}.json</code> first, then <code>api/crypto/{{slug}}.json</code>. A 404 means the ticker is not in that universe.</li>
    <li>To search by company name, download <code>api/index.json</code> once (about 1&nbsp;MB gzipped) and filter locally.</li>
    <li>Treat <code>market_cap</code>, <code>employees</code> and other metadata as a reference snapshot (see <code>updated_at_utc</code>), not live market data.</li>
    <li>For prices, pass <code>yahoo_symbol</code> to a market-data API. TickerAtlas does not serve quotes.</li>
    <li>When you cite this data, name TickerAtlas and the upstream source (see <a href="sources.html">Sources</a>).</li>
  </ol>
</section>

<section class="block">
  <h2>Stock &amp; ETF record fields</h2>
  <div class="table-scroll"><table class="tbl"><thead><tr><th>Field</th><th>Type</th><th>Description</th></tr></thead><tbody>{rows}</tbody></table></div>
  <h2 style="margin-top:28px">Crypto record fields</h2>
  <div class="table-scroll"><table class="tbl"><thead><tr><th>Field</th><th>Description</th></tr></thead><tbody>
    <tr><td><code>id</code></td><td><code>crypto:&lt;ticker&gt;</code></td></tr>
    <tr><td><code>ticker</code>, <code>name</code></td><td>Symbol and project name from crypti/cryptocurrencies</td></tr>
    <tr><td><code>source</code>, <code>source_commit</code>, <code>source_license</code></td><td>Upstream repository, the exact commit used, and its license (MIT)</td></tr>
    <tr><td><code>original_source</code></td><td>CryptoCompare, the upstream origin of the list</td></tr>
  </tbody></table></div>
</section>
"""
    return page(
        title="API & AI access | TickerAtlas",
        description="Static JSON API for stock, ETF and crypto tickers. Endpoints, URL rules, field reference and examples for AI agents and developers.",
        prefix="",
        canonical=site_url + "docs.html",
        body=body,
        built=built,
        current="docs",
    )


def sources_page(stats: Dict[str, Any], site_url: str, built: str) -> str:
    cards = "".join(
        f'<div class="card"><h3><a href="{e(s["url"])}">{e(s["name"])}</a></h3><p>{e(s["provides"])}</p>'
        f'<p><strong>Terms:</strong> {e(s["license"])}</p></div>'
        for s in SOURCES
    )
    commit = stats.get("crypto_source_commit") or "master"
    body = f"""
<section class="hero">
  <p class="eyebrow">Attribution</p>
  <h1>Sources &amp; licenses</h1>
  <p class="lede">TickerAtlas compiles public reference data from the sources below. Credit goes to them. Please cite them
  alongside TickerAtlas when you use this data.</p>
</section>
<section class="block"><div class="grid-2">{cards}</div></section>
<section class="block">
  <h2>How the data is built</h2>
  <ol>
    <li>Every day, GitHub Actions downloads the NASDAQ Trader symbol directories (<code>nasdaqlisted.txt</code>, <code>otherlisted.txt</code>).</li>
    <li>Each symbol is enriched with company metadata from Yahoo Finance through yfinance and upserted into <code>data/All_Stocks.sqlite</code>.</li>
    <li>The crypto list is downloaded from <a href="https://github.com/crypti/cryptocurrencies/tree/{e(commit)}">crypti/cryptocurrencies @ <code>{e(commit[:12])}</code></a>.</li>
    <li>Both are merged into <code>data/All_Assets.csv</code>, and this website and API are regenerated from the result.</li>
  </ol>
  <p>Source code: <a href="{REPO_URL}">{REPO_URL}</a></p>
</section>
<section class="block">
  <h2>Disclaimer</h2>
  <p>TickerAtlas is reference data provided as-is, without warranty. It is not investment advice. Fields can be incomplete,
  delayed or wrong; check anything important against the primary source. Crypto symbols are not standardized, so one symbol can
  refer to more than one project.</p>
</section>
<section class="block">
  <h2>crypti/cryptocurrencies license</h2>
  <pre><code>{e(CRYPTI_MIT)}</code></pre>
</section>
"""
    return page(
        title="Sources & licenses | TickerAtlas",
        description="Where TickerAtlas data comes from: NASDAQ Trader, Yahoo Finance via yfinance, crypti/cryptocurrencies (MIT) and CryptoCompare.",
        prefix="",
        canonical=site_url + "sources.html",
        body=body,
        built=built,
        current="sources",
    )


def not_found_page(site_url: str, built: str) -> str:
    # 404.html is served at arbitrary depths, so use absolute paths.
    base = site_url
    body = (
        '<section class="hero"><p class="eyebrow">404</p><h1>No such ticker page.</h1>'
        '<p class="lede">The ticker may not be in this universe, or the URL slug may be off. '
        f'<a href="{e(base)}index.html">Search all tickers</a> or read the <a href="{e(base)}docs.html">URL rules</a>.</p></section>'
    )
    return page(
        title="Not found | TickerAtlas",
        description="Page not found.",
        prefix=base,
        canonical=base + "404.html",
        body=body,
        built=built,
    )


# --------------------------------------------------------------------------- text / machine files


def llms_txt(stats: Dict[str, Any], site_url: str) -> str:
    n = stats["counts"]
    return f"""# TickerAtlas

> Open reference dataset of {n['total']:,} tickers: {n['stock']:,} stocks, {n['etf']:,} ETFs and {n['crypto']:,} cryptocurrencies, with names, exchanges, sectors, industries, countries, market caps and company descriptions. Static files, no API key, CORS enabled. Regenerated daily. Last build: {stats['generated_at']}.

Base URL: {site_url}

## Look up a ticker
- Stock or ETF: {site_url}api/stocks/{{slug}}.json  (e.g. {site_url}api/stocks/AAPL.json)
- Cryptocurrency: {site_url}api/crypto/{{slug}}.json  (e.g. {site_url}api/crypto/BTC.json)
- Human-readable pages: {site_url}stocks/{{slug}}.html and {site_url}crypto/{{slug}}.html
- slug = the ticker, with every character outside A-Z a-z 0-9 . - replaced by "_" + its UTF-8 bytes in uppercase hex ("$MAID" -> "_24MAID", "ABR$D" -> "ABR_24D"). Case-sensitive.
- The same symbol can be both a stock and a crypto (e.g. SOL). Unique key = id = "<asset_class>:<ticker>".
- HTTP 404 = ticker not in that universe.

## Bulk data
- [Search index (JSON, columnar)]({site_url}api/index.json): fields = id, ticker, name, asset_class, exchange, sector, country, slug
- [All assets (CSV)]({site_url}data/All_Assets.csv): id, ticker, name, asset_class, exchange, country, sector, source
- [All stock/ETF records (JSON)]({site_url}api/stocks.json)
- [All crypto records (JSON)]({site_url}api/crypto.json)
- [Stock/ETF SQLite database]({site_url}data/All_Stocks.sqlite): table "stocks"
- [Every ticker and name, one per line]({site_url}llms-full.txt)
- [OpenAPI spec]({site_url}openapi.json)

## Docs
- [API & AI access]({site_url}docs.html): endpoints, URL rules, field reference, examples
- [Sources & licenses]({site_url}sources.html)
- [Source code]({REPO_URL})

## Sources
- US listings: NASDAQ Trader symbol directory (nasdaqlisted.txt, otherlisted.txt) https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs
- Company metadata: Yahoo Finance via yfinance https://github.com/ranaroussi/yfinance
- Cryptocurrencies: crypti/cryptocurrencies (MIT, (c) Michael Wuergler) https://github.com/crypti/cryptocurrencies, generated from the CryptoCompare coin list https://www.cryptocompare.com/

## Notes
- Reference data, not live prices. To get quotes, pass the "yahoo_symbol" field to a market-data API.
- Not investment advice. Fields may be missing or stale; check "updated_at_utc".
"""


def llms_full_txt(stocks: List[Dict[str, Any]], crypto: List[Dict[str, Any]], stats: Dict[str, Any], site_url: str) -> str:
    lines = [
        "# TickerAtlas: full ticker listing",
        f"# {stats['counts']['total']:,} assets, built {stats['generated_at']}. Tab-separated: asset_class, ticker, name, exchange, sector.",
        f"# Detail JSON: {site_url}api/stocks/<slug>.json (stock/etf) or {site_url}api/crypto/<slug>.json (crypto). See {site_url}llms.txt",
        "asset_class\tticker\tname\texchange\tsector",
    ]

    def cell(x: Any) -> str:
        return "" if x is None else str(x).replace("\t", " ").replace("\n", " ")

    for r in stocks:
        lines.append("\t".join(cell(x) for x in (r["asset_class"], r["ticker"], r["name"], r["exchange_name"] or r["exchange"], r["sector"])))
    for r in crypto:
        lines.append("\t".join(cell(x) for x in ("crypto", r["ticker"], r["name"], "", "")))
    return "\n".join(lines) + "\n"


def openapi_spec(site_url: str, stats: Dict[str, Any]) -> Dict[str, Any]:
    slug_param = {
        "name": "slug",
        "in": "path",
        "required": True,
        "description": "Ticker; characters outside [A-Za-z0-9.-] are encoded as '_' + UTF-8 hex (e.g. '$MAID' -> '_24MAID').",
        "schema": {"type": "string"},
    }
    nullable_str = {"type": ["string", "null"]}
    stock_schema = {
        "type": "object",
        "required": ["id", "ticker", "name", "asset_class"],
        "properties": {
            "id": {"type": "string", "example": "stock:AAPL"},
            "ticker": {"type": "string"},
            "name": {"type": "string"},
            "asset_class": {"type": "string", "example": "stock"},
            "quote_type": nullable_str,
            "exchange": nullable_str,
            "exchange_name": nullable_str,
            "currency": nullable_str,
            "country": nullable_str,
            "sector": nullable_str,
            "industry": nullable_str,
            "website": nullable_str,
            "employees": {"type": ["integer", "null"]},
            "market_cap": {"type": ["integer", "null"]},
            "first_trade_date": nullable_str,
            "age_years_since_first_trade": {"type": ["number", "null"]},
            "business_summary": nullable_str,
            "listing_source": nullable_str,
            "updated_at_utc": nullable_str,
            "yahoo_symbol": {"type": "string"},
            "slug": {"type": "string"},
            "page": {"type": "string"},
            "json": {"type": "string"},
        },
    }
    crypto_schema = {
        "type": "object",
        "required": ["id", "ticker", "name", "asset_class"],
        "properties": {
            "id": {"type": "string", "example": "crypto:BTC"},
            "ticker": {"type": "string"},
            "name": {"type": "string"},
            "asset_class": {"type": "string", "const": "crypto"},
            "source": {"type": "string"},
            "source_commit": nullable_str,
            "source_license": {"type": "string"},
            "original_source": {"type": "string"},
            "slug": {"type": "string"},
            "page": {"type": "string"},
            "json": {"type": "string"},
        },
    }

    def ok(schema: Dict[str, Any], desc: str) -> Dict[str, Any]:
        return {"200": {"description": desc, "content": {"application/json": {"schema": schema}}}}

    return {
        "openapi": "3.1.0",
        "info": {
            "title": "TickerAtlas static API",
            "version": "1.0.0",
            "description": "Read-only static JSON for stock, ETF and cryptocurrency tickers. No authentication. "
            "Sources: NASDAQ Trader, Yahoo Finance (yfinance), crypti/cryptocurrencies (MIT).",
        },
        "servers": [{"url": site_url.rstrip("/")}],
        "paths": {
            "/api/stocks/{slug}.json": {
                "get": {
                    "operationId": "getStock",
                    "summary": "Get one stock or ETF by ticker",
                    "parameters": [slug_param],
                    "responses": {**ok({"$ref": "#/components/schemas/Stock"}, "Stock record"), "404": {"description": "Ticker not found"}},
                }
            },
            "/api/crypto/{slug}.json": {
                "get": {
                    "operationId": "getCrypto",
                    "summary": "Get one cryptocurrency by symbol",
                    "parameters": [slug_param],
                    "responses": {**ok({"$ref": "#/components/schemas/Crypto"}, "Crypto record"), "404": {"description": "Symbol not found"}},
                }
            },
            "/api/index.json": {
                "get": {
                    "operationId": "getIndex",
                    "summary": "Compact index of every asset (columnar)",
                    "responses": ok(
                        {
                            "type": "object",
                            "properties": {
                                "generated_at": {"type": "string"},
                                "fields": {"type": "array", "items": {"type": "string"}},
                                "rows": {"type": "array", "items": {"type": "array"}},
                            },
                        },
                        "Index",
                    ),
                }
            },
            "/api/stocks.json": {
                "get": {
                    "operationId": "listStocks",
                    "summary": "Every stock and ETF record",
                    "responses": ok({"type": "array", "items": {"$ref": "#/components/schemas/Stock"}}, "All stocks"),
                }
            },
            "/api/crypto.json": {
                "get": {
                    "operationId": "listCrypto",
                    "summary": "Every crypto record",
                    "responses": ok({"type": "array", "items": {"$ref": "#/components/schemas/Crypto"}}, "All crypto"),
                }
            },
            "/api/meta.json": {
                "get": {"operationId": "getMeta", "summary": "Counts, build time and sources", "responses": ok({"type": "object"}, "Metadata")}
            },
        },
        "components": {"schemas": {"Stock": stock_schema, "Crypto": crypto_schema}},
    }


def sitemap(urls: List[str]) -> str:
    body = "".join(f"<url><loc>{html.escape(u)}</loc></url>\n" for u in urls)
    return f'<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n{body}</urlset>\n'


FAVICON = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32" fill="none" stroke="#0f6e6a" stroke-width="2.4">'
    '<circle cx="16" cy="16" r="13"/><ellipse cx="16" cy="16" rx="5.5" ry="13"/><path d="M3.5 12h25M3.5 20h25"/></svg>\n'
)


# --------------------------------------------------------------------------- main


def main() -> int:
    p = argparse.ArgumentParser(description="Build the TickerAtlas static site")
    p.add_argument("--data-dir", default=os.path.join(ROOT, "data"))
    p.add_argument("--assets-dir", default=os.path.join(ROOT, "site", "assets"))
    p.add_argument("--out", default=os.path.join(ROOT, "_site"))
    p.add_argument("--site-url", default=os.environ.get("SITE_URL") or DEFAULT_SITE_URL)
    p.add_argument("--no-detail-pages", action="store_true", help="Skip per-ticker HTML/JSON (fast preview)")
    args = p.parse_args()

    site_url = args.site_url.rstrip("/") + "/"
    out = args.out
    if os.path.exists(out):
        shutil.rmtree(out)
    os.makedirs(out)

    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    built = now.strftime("%Y-%m-%d %H:%M UTC")

    crypto_commit = None
    crypto_json = os.path.join(args.data_dir, "Crypto.json")
    if os.path.exists(crypto_json):
        with open(crypto_json, encoding="utf-8") as f:
            crypto_commit = json.load(f).get("meta", {}).get("source_commit")

    stocks = load_stock_records(os.path.join(args.data_dir, "All_Stocks.csv"))
    crypto = load_crypto_records(os.path.join(args.data_dir, "Crypto.csv"), crypto_commit)
    if not stocks and not crypto:
        print("Error: no data found in", args.data_dir, file=sys.stderr)
        return 1
    check_case_collisions(stocks, "stock")
    check_case_collisions(crypto, "crypto")

    counts = Counter(r["asset_class"] for r in stocks)
    stats = {
        "generated_at": now.isoformat(),
        "counts": {
            "total": len(stocks) + len(crypto),
            "stock": sum(v for k, v in counts.items() if k not in ("etf",)),
            "etf": counts.get("etf", 0),
            "crypto": len(crypto),
            "by_asset_class": dict(counts, crypto=len(crypto)),
        },
        "crypto_source_commit": crypto_commit,
        "sources": SOURCES,
        "site_url": site_url,
        "repository": REPO_URL,
    }

    # Static assets
    shutil.copytree(args.assets_dir, os.path.join(out, "assets"))
    write(os.path.join(out, "assets", "favicon.svg"), FAVICON)
    write(os.path.join(out, ".nojekyll"), "")

    # Downloads
    os.makedirs(os.path.join(out, "data"), exist_ok=True)
    for fname, _ in DOWNLOADS:
        src = os.path.join(args.data_dir, fname)
        if os.path.exists(src):
            shutil.copy2(src, os.path.join(out, "data", fname))

    # API: index, meta, bulk
    fields = ["id", "ticker", "name", "asset_class", "exchange", "sector", "country", "slug"]
    rows: List[List[Any]] = []
    for r in stocks:
        rows.append([r["id"], r["ticker"], r["name"], r["asset_class"], r["exchange_name"] or r["exchange"], r["sector"], r["country"], r["slug"] if r["slug"] != r["ticker"] else ""])
    for r in crypto:
        rows.append([r["id"], r["ticker"], r["name"], "crypto", None, None, None, r["slug"] if r["slug"] != r["ticker"] else ""])
    write_json(
        os.path.join(out, "api", "index.json"),
        {
            "generated_at": stats["generated_at"],
            "count": len(rows),
            "fields": fields,
            "note": "slug is empty when identical to ticker. Detail JSON: api/stocks/<slug>.json or api/crypto/<slug>.json.",
            "rows": rows,
        },
    )
    write_json(os.path.join(out, "api", "meta.json"), stats, compact=False)
    write_json(os.path.join(out, "api", "stocks.json"), stocks)
    write_json(os.path.join(out, "api", "crypto.json"), crypto)

    # Detail pages
    stock_by_ticker = {r["ticker"]: r for r in stocks}
    crypto_by_ticker = {r["ticker"]: r for r in crypto}
    urls = [site_url, site_url + "docs.html", site_url + "sources.html"]
    if not args.no_detail_pages:
        for r in stocks:
            write_json(os.path.join(out, "api", "stocks", r["slug"] + ".json"), r, compact=False)
            if write(os.path.join(out, "stocks", r["slug"] + ".html"), stock_page(r, site_url, built, crypto_by_ticker.get(r["ticker"]))):
                urls.append(site_url + r["page"])
        for r in crypto:
            write_json(os.path.join(out, "api", "crypto", r["slug"] + ".json"), r, compact=False)
            if write(os.path.join(out, "crypto", r["slug"] + ".html"), crypto_page(r, site_url, built, stock_by_ticker.get(r["ticker"]))):
                urls.append(site_url + r["page"])

    # Top-level pages
    write(os.path.join(out, "index.html"), index_page(stats, site_url, built))
    write(os.path.join(out, "docs.html"), docs_page(stats, site_url, built))
    write(os.path.join(out, "sources.html"), sources_page(stats, site_url, built))
    write(os.path.join(out, "404.html"), not_found_page(site_url, built))

    # Machine-readable extras
    write(os.path.join(out, "llms.txt"), llms_txt(stats, site_url))
    write(os.path.join(out, "llms-full.txt"), llms_full_txt(stocks, crypto, stats, site_url))
    write_json(os.path.join(out, "openapi.json"), openapi_spec(site_url, stats), compact=False)
    write(os.path.join(out, "robots.txt"), f"User-agent: *\nAllow: /\nSitemap: {site_url}sitemap.xml\n")
    write(os.path.join(out, "sitemap.xml"), sitemap(urls))

    print(
        f"Built {out}: {len(stocks)} stocks/ETFs, {len(crypto)} crypto, "
        f"{len(urls)} pages in sitemap, site URL {site_url}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
