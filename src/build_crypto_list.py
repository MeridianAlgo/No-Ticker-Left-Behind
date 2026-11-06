"""
Build the TickerAtlas cryptocurrency universe.

Source: crypti/cryptocurrencies (MIT License, (c) Michael Wuergler)
    https://github.com/crypti/cryptocurrencies
which itself is generated from the CryptoCompare coin list
    https://www.cryptocompare.com/api/data/coinlist/

The upstream file is a flat JSON object of {symbol: name}. This script pins the
download to the latest upstream commit (so every output row is traceable to an
exact upstream version) and writes:

    data/Crypto.csv          ticker,name,asset_class,source
    data/Crypto.json         {"meta": {...}, "crypto": [{"ticker", "name"}, ...]}
    data/Crypto_Tickers.txt  one symbol per line

Outputs are deterministic: if upstream has not changed, the files do not change,
so the daily workflow does not create noisy commits.
"""

import argparse
import csv
import json
import os
import sys
from typing import Dict, Optional

import requests

UPSTREAM_REPO = "crypti/cryptocurrencies"
UPSTREAM_BRANCH = "master"
UPSTREAM_FILE = "cryptocurrencies.json"
UPSTREAM_URL = f"https://github.com/{UPSTREAM_REPO}"
UPSTREAM_LICENSE = "MIT"
ORIGINAL_SOURCE = "CryptoCompare coin list (https://www.cryptocompare.com/)"
SOURCE_LABEL = "crypti/cryptocurrencies"

HEADERS = {"User-Agent": "TickerAtlas/1.0 (+https://github.com/MeridianAlgo/TickerAtlas)"}


def latest_upstream_commit(timeout_s: int = 30) -> Optional[str]:
    """Return the latest commit SHA of the upstream branch, or None if unavailable."""
    url = f"https://api.github.com/repos/{UPSTREAM_REPO}/commits/{UPSTREAM_BRANCH}"
    headers = dict(HEADERS, Accept="application/vnd.github+json")
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        r = requests.get(url, headers=headers, timeout=timeout_s)
        r.raise_for_status()
        return str(r.json()["sha"])
    except Exception as e:  # network / rate limit: fall back to branch name
        print(f"Warning: could not resolve upstream commit ({e}); using '{UPSTREAM_BRANCH}'")
        return None


def fetch_crypto_map(ref: str, timeout_s: int = 60) -> Dict[str, str]:
    url = f"https://raw.githubusercontent.com/{UPSTREAM_REPO}/{ref}/{UPSTREAM_FILE}"
    r = requests.get(url, headers=HEADERS, timeout=timeout_s)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict) or not data:
        raise RuntimeError(f"Unexpected payload from {url}: expected a non-empty JSON object")
    return data


def clean(data: Dict[str, str]) -> Dict[str, str]:
    """Strip whitespace, drop empty symbols, and sort case-insensitively."""
    out: Dict[str, str] = {}
    for sym, name in data.items():
        s = str(sym).strip()
        n = " ".join(str(name or "").split())
        if not s:
            continue
        out.setdefault(s, n or s)
    return dict(sorted(out.items(), key=lambda kv: (kv[0].upper(), kv[0])))


def write_outputs(data: Dict[str, str], out_dir: str, commit: Optional[str]) -> None:
    os.makedirs(out_dir, exist_ok=True)

    csv_path = os.path.join(out_dir, "Crypto.csv")
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["ticker", "name", "asset_class", "source"])
        for sym, name in data.items():
            w.writerow([sym, name, "crypto", SOURCE_LABEL])
    print(f"Wrote {csv_path} ({len(data)} rows)")

    json_path = os.path.join(out_dir, "Crypto.json")
    payload = {
        "meta": {
            "dataset": "TickerAtlas crypto universe",
            "count": len(data),
            "source": UPSTREAM_URL,
            "source_file": UPSTREAM_FILE,
            "source_commit": commit or UPSTREAM_BRANCH,
            "source_license": UPSTREAM_LICENSE,
            "original_source": ORIGINAL_SOURCE,
            "note": "Symbols are not globally unique across asset classes; "
            "'SOL' may be both a stock and a cryptocurrency.",
        },
        "crypto": [{"ticker": s, "name": n} for s, n in data.items()],
    }
    with open(json_path, "w", encoding="utf-8", newline="\n") as f:
        # One record per line: compact, but still diff- and grep-friendly.
        f.write('{\n"meta": ' + json.dumps(payload["meta"], ensure_ascii=False, indent=2))
        f.write(',\n"crypto": [\n')
        rows = [json.dumps(r, ensure_ascii=False) for r in payload["crypto"]]
        f.write(",\n".join(rows))
        f.write("\n]\n}\n")
    print(f"Wrote {json_path}")

    txt_path = os.path.join(out_dir, "Crypto_Tickers.txt")
    with open(txt_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(f"# TickerAtlas crypto universe (source: {UPSTREAM_URL}, MIT)\n")
        f.write(f"# Total: {len(data)} symbols\n")
        for sym in data:
            f.write(sym + "\n")
    print(f"Wrote {txt_path}")


def main() -> int:
    p = argparse.ArgumentParser(description="Build the crypto universe from crypti/cryptocurrencies")
    p.add_argument("--out-dir", default="data")
    p.add_argument(
        "--ref",
        default=None,
        help="Upstream git ref to download (default: latest commit on master)",
    )
    args = p.parse_args()

    commit = args.ref or latest_upstream_commit()
    try:
        raw = fetch_crypto_map(commit or UPSTREAM_BRANCH)
    except Exception as e:
        print(f"Error downloading crypto list: {e}", file=sys.stderr)
        return 1

    data = clean(raw)
    write_outputs(data, args.out_dir, commit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
