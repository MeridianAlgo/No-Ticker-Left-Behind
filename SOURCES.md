# Sources & third-party notices

TickerAtlas compiles public reference data. All credit for the underlying data goes to the sources below.

| Data | Source | Terms |
| --- | --- | --- |
| US listing universe (Nasdaq, NYSE, NYSE American, NYSE Arca, Cboe, IEX) | [NASDAQ Trader symbol directory](https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs): `nasdaqlisted.txt`, `otherlisted.txt` | Public symbol directory published by Nasdaq, Inc. |
| Company metadata (name, sector, industry, country, website, employees, market cap, description) | [Yahoo Finance](https://finance.yahoo.com/), retrieved with [yfinance](https://github.com/ranaroussi/yfinance) (Apache-2.0) | Subject to Yahoo's terms of service; intended for personal and research use |
| Cryptocurrency symbols & names | [crypti/cryptocurrencies](https://github.com/crypti/cryptocurrencies) (`cryptocurrencies.json`) | MIT License (below) |
| Origin of the crypto list | [CryptoCompare](https://www.cryptocompare.com/) coin list. crypti/cryptocurrencies is generated from it | Attribution only; TickerAtlas does not query CryptoCompare |
| Optional global symbol lists | [EODHD](https://eodhd.com/) (only when `EODHD_API_KEY` is set) | EODHD terms |

`data/Crypto.json` records the exact upstream commit of crypti/cryptocurrencies used for each build (`meta.source_commit`).

## crypti/cryptocurrencies, MIT License

```
The MIT License (MIT)

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
THE SOFTWARE.
```

## Disclaimer

TickerAtlas is provided as-is, without warranty, and is not investment advice. Data can be incomplete, delayed or wrong. Check anything important against the primary source.
