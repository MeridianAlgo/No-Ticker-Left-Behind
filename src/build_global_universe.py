import argparse
import os
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
import requests


def _read_tickers_from_text(path: str) -> List[str]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        lines = [ln.strip() for ln in f.read().splitlines()]
    out: List[str] = []
    for ln in lines:
        if not ln or ln.startswith("#"):
            continue
        out.append(ln)
    return out


def _to_yahoo_ticker(exchange_code: str, symbol: str) -> str:
    s = str(symbol).strip()
    if not s:
        return s
    ex = exchange_code.strip().upper()
    # EODHD returns symbols without Yahoo suffixes for many non-US exchanges.
    # Map a few common ones.
    suffix_map = {
        "TO": ".TO",  # Toronto
        "SW": ".SW",  # SIX Swiss
        "XETRA": ".DE",  # Xetra Germany
    }
    if ex in suffix_map:
        # Avoid double-suffixing
        if s.upper().endswith(suffix_map[ex]):
            return s
        return s + suffix_map[ex]
    return s


def _read_tickers_from_csv(path: str, column: str = "ticker") -> List[str]:
    if not os.path.exists(path):
        return []
    df = pd.read_csv(path)
    if column not in df.columns:
        raise RuntimeError(f"CSV {path} missing column '{column}'")
    tickers = df[column].dropna().astype(str).str.strip().tolist()
    return [t for t in tickers if t]


def _write_tickers(path: str, tickers: Iterable[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    uniq = sorted({t.strip() for t in tickers if str(t).strip()})
    with open(path, "w", encoding="utf-8") as f:
        f.write("# Auto-generated global ticker universe (Yahoo Finance symbols)\n")
        f.write("# Edit sources under inputs/global_sources/ and re-run this script.\n")
        for t in uniq:
            f.write(t + "\n")


def _eodhd_exchanges_list(api_key: str, timeout_s: int = 60) -> List[Dict[str, str]]:
    url = "https://eodhd.com/api/exchanges-list/"
    r = requests.get(url, params={"api_token": api_key, "fmt": "json"}, timeout=timeout_s)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        return []
    return [d for d in data if isinstance(d, dict)]


def _eodhd_exchange_symbols(api_key: str, exchange_code: str, timeout_s: int = 60) -> List[str]:
    # Requires an API key. See: https://eodhd.com/
    url = f"https://eodhd.com/api/exchange-symbol-list/{exchange_code}"
    r = requests.get(url, params={"api_token": api_key, "fmt": "json"}, timeout=timeout_s)
    r.raise_for_status()
    data = r.json()
    out: List[str] = []
    for row in data:
        sym = str(row.get("Code") or "").strip()
        if sym:
            out.append(sym)
    return out


def _normalize_exchange_code(code: str) -> str:
    c = code.strip().upper()
    aliases = {
        # Common user-friendly aliases
        "GER": "XETRA",
        "DE": "XETRA",
        "XETRA": "XETRA",
        # Switzerland (SIX Swiss Exchange)
        "SWX": "SW",
        "SW": "SW",
        "SIX": "SW",
        # Canada (Toronto)
        "TSX": "TO",
        "TOR": "TO",
    }
    return aliases.get(c, c)


def _suggest_exchange_code(exchanges: List[Dict[str, str]], wanted: str) -> Optional[str]:
    w = wanted.strip().lower()
    # Try exact code match
    for d in exchanges:
        if str(d.get("Code", "")).strip().lower() == w:
            return str(d.get("Code", "")).strip().upper()
    # Try by name contains
    for d in exchanges:
        name = str(d.get("Name", "")).lower()
        if w and w in name:
            return str(d.get("Code", "")).strip().upper()
    return None


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--out", default=os.path.join("inputs", "global_tickers.txt"))
    p.add_argument(
        "--source-dir",
        default=os.path.join("inputs", "global_sources"),
        help="Directory containing .txt/.csv files to merge into the universe",
    )
    p.add_argument(
        "--eodhd-api-key",
        default=os.environ.get("EODHD_API_KEY", ""),
        help="Optional: EODHD API key (or set env EODHD_API_KEY)",
    )
    p.add_argument(
        "--eodhd-exchanges",
        default="",
        help="Optional: comma-separated EODHD exchange codes (e.g. US,TO,GER,SWX,TSE)",
    )
    args = p.parse_args()

    tickers: Set[str] = set()
    failures: List[Tuple[str, str]] = []

    # Merge local source files
    if os.path.isdir(args.source_dir):
        for name in os.listdir(args.source_dir):
            path = os.path.join(args.source_dir, name)
            if os.path.isdir(path):
                continue
            if name.lower().endswith(".txt"):
                tickers.update(_read_tickers_from_text(path))
            elif name.lower().endswith(".csv"):
                # expects a 'ticker' column by default
                tickers.update(_read_tickers_from_csv(path))

    # Optional provider: EODHD exchange symbol lists
    if args.eodhd_exchanges:
        if not args.eodhd_api_key:
            raise RuntimeError("--eodhd-exchanges provided but no EODHD API key was set")

        exchanges_meta: List[Dict[str, str]] = []
        try:
            exchanges_meta = _eodhd_exchanges_list(args.eodhd_api_key)
        except Exception:
            exchanges_meta = []

        for exch in [x.strip() for x in args.eodhd_exchanges.split(",") if x.strip()]:
            requested = exch
            code = _normalize_exchange_code(exch)
            try:
                syms = _eodhd_exchange_symbols(args.eodhd_api_key, code)
                for sym in syms:
                    tickers.add(_to_yahoo_ticker(code, sym))
                print(f"EODHD OK: {requested} -> {code} ({len(syms)} tickers)")
            except requests.HTTPError as e:
                msg = f"HTTP {e.response.status_code}" if getattr(e, "response", None) is not None else "HTTP error"
                failures.append((requested, msg))
                suggestion = _suggest_exchange_code(exchanges_meta, requested) if exchanges_meta else None
                if suggestion and suggestion != code:
                    print(f"EODHD FAIL: {requested} -> {code} ({msg}). Try exchange code: {suggestion}")
                else:
                    print(f"EODHD FAIL: {requested} -> {code} ({msg}). Skipping.")
                continue
            except Exception as e:
                failures.append((requested, str(e)))
                print(f"EODHD FAIL: {requested} -> {code} ({e}). Skipping.")
                continue

    _write_tickers(args.out, tickers)
    print(f"Wrote {len(tickers)} tickers to {args.out}")
    if failures:
        print("Some exchanges failed:")
        for ex, msg in failures:
            print(f"  {ex}: {msg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
