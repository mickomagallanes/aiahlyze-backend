import yfinance as yf
import pandas as pd
import requests
import json
import time
import os
from datetime import datetime
import math
from io import StringIO

# --- CONFIGURATION ---

DAILY_HISTORY_DAYS = 30
DAILY_HISTORY_PERIOD = "3mo"

# Top Philippine Stocks - Using ADR/OTC tickers that trade on US markets
# NOTE: Yahoo Finance does not support PSE stocks (.PS suffix)
# Using ADR (American Depositary Receipt) and OTC tickers instead
PH_STOCKS = [
    {"symbol": "PHI", "name": "PLDT Inc. (NYSE ADR)"},
    {"symbol": "BPHLY", "name": "Bank of the Philippine Islands (ADR)"},
    {"symbol": "BDOUY", "name": "BDO Unibank (ADR)"},
    {"symbol": "BDOUF", "name": "BDO Unibank (OTC)"},
    {"symbol": "AYALY", "name": "Ayala Corporation (ADR)"},
    {"symbol": "ABTZY", "name": "Aboitiz Equity Ventures (ADR)"},
    {"symbol": "ABZPY", "name": "Aboitiz Power Corporation (ADR)"},
    {"symbol": "ALGGY", "name": "Alliance Global Group (ADR)"},
    {"symbol": "ACMDY", "name": "Atlas Consolidated Mining (ADR)"},
    {"symbol": "AYAAY", "name": "Ayala Land (ADR)"},
    {"symbol": "MGAWY", "name": "Megaworld Corporation (ADR)"},
    {"symbol": "PRGLY", "name": "Puregold Price Club (ADR)"},
    {"symbol": "CEBUY", "name": "Cebu Air (ADR)"},
    {"symbol": "DMCHY", "name": "DMCI Holdings (ADR)"},
    {"symbol": "MTPOY", "name": "Metrobank (ADR)"},
    {"symbol": "JBFCF", "name": "Jollibee Foods Corporation (OTC)"},
    {"symbol": "SPHXF", "name": "SM Prime Holdings (OTC)"},
    {"symbol": "GTMEF", "name": "Globe Telecom (OTC)"},
    {"symbol": "UVRBF", "name": "Universal Robina Corporation (OTC)"},
    {"symbol": "SYBJF", "name": "Security Bank Corporation (OTC)"},
    {"symbol": "RBLAY", "name": "Robinsons Land Corporation (ADR)"},
    {"symbol": "MNDDF", "name": "Monde Nissin Corporation (OTC)"},
]

# Major Global Indices (no commodities/metals, no PSI - always fails)
INDICES = [
    {"symbol": "^GSPC", "name": "S&P 500 Index"},
    {"symbol": "^IXIC", "name": "NASDAQ Composite"},
    {"symbol": "^DJI", "name": "Dow Jones Industrial Average"},
    {"symbol": "^N225", "name": "Nikkei 225 (Japan)"},
    {"symbol": "^HSI", "name": "Hang Seng Index (Hong Kong)"},
    {"symbol": "^FTSE", "name": "FTSE 100 (UK)"},
    {"symbol": "^GDAXI", "name": "DAX (Germany)"},
    {"symbol": "^FCHI", "name": "CAC 40 (France)"},
    {"symbol": "^STOXX50E", "name": "Euro Stoxx 50"},
    {"symbol": "^AXJO", "name": "ASX 200 (Australia)"},
    {"symbol": "^BVSP", "name": "Bovespa Index (Brazil)"},
]

# Metals and Commodities (separate tracking)
# Logo URLs point to uploaded images on GitHub
METALS_COMMODITIES = [
    {
        "symbol": "GC=F",
        "name": "Gold",
        "logo_url": "https://raw.githubusercontent.com/mickomagallanes/aiahlyze-backend/refs/heads/main/gold.png",
    },
    {
        "symbol": "SI=F",
        "name": "Silver",
        "logo_url": "https://raw.githubusercontent.com/mickomagallanes/aiahlyze-backend/refs/heads/main/silver.png",
    },
    {
        "symbol": "CL=F",
        "name": "Crude Oil (WTI)",
        "logo_url": "https://raw.githubusercontent.com/mickomagallanes/aiahlyze-backend/refs/heads/main/oil.png",
    },
    {
        "symbol": "BZ=F",
        "name": "Brent Crude Oil",
        "logo_url": "https://raw.githubusercontent.com/mickomagallanes/aiahlyze-backend/refs/heads/main/brent.png",
    },
    {
        "symbol": "NG=F",
        "name": "Natural Gas",
        "logo_url": "https://raw.githubusercontent.com/mickomagallanes/aiahlyze-backend/refs/heads/main/natural-gas.png",
    },
    {
        "symbol": "HG=F",
        "name": "Copper",
        "logo_url": "https://raw.githubusercontent.com/mickomagallanes/aiahlyze-backend/refs/heads/main/copper.png",
    },
    {
        "symbol": "PL=F",
        "name": "Platinum",
        "logo_url": "https://raw.githubusercontent.com/mickomagallanes/aiahlyze-backend/refs/heads/main/platinum.png",
    },
    {
        "symbol": "PA=F",
        "name": "Palladium",
        "logo_url": "https://raw.githubusercontent.com/mickomagallanes/aiahlyze-backend/refs/heads/main/palladium.png",
    },
]


def save_json(data, filename):
    """Helper to save a dictionary to a JSON file."""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    item_count = len(
        data.get("prices", data.get("tickers", data.get("histories", [])))
    )
    print(f"✅ Successfully saved {item_count} items to {filename}")


def _utc_now_iso():
    return datetime.utcnow().isoformat()


def _safe_float(value):
    """Return a float if value is finite, otherwise None."""
    try:
        val = float(value)
        if math.isnan(val) or math.isinf(val):
            return None
        return val
    except Exception:
        return None


def _series_to_daily_points(close_series, days=DAILY_HISTORY_DAYS):
    """Convert a Close price series to sorted daily point objects."""
    if close_series is None or len(close_series) == 0:
        return []

    close_series = close_series.dropna()
    if len(close_series) == 0:
        return []

    points = []
    for idx, value in close_series.items():
        close = _safe_float(value)
        if close is None:
            continue

        try:
            date_str = pd.Timestamp(idx).strftime("%Y-%m-%d")
        except Exception:
            continue

        points.append({"date": date_str, "close": round(close, 6)})

    points.sort(key=lambda x: x["date"])
    if len(points) > days:
        points = points[-days:]

    return points


def _fallback_daily_points(symbol, days=DAILY_HISTORY_DAYS, period=DAILY_HISTORY_PERIOD):
    """Single-ticker history fallback."""
    try:
        hist = yf.Ticker(symbol).history(period=period, interval="1d")
        if hist is None or hist.empty:
            return []
        return _series_to_daily_points(hist.get("Close"), days=days)
    except Exception:
        return []


def fetch_logo_url(symbol):
    """
    Try to get a logo URL for a ticker via yfinance website field.
    Uses multiple fallback services: Google Favicons, Unavatar, DuckDuckGo.
    Returns the first working logo URL, or None if all fail.
    """
    try:
        info = yf.Ticker(symbol).info or {}
        website = info.get("website") or ""
        if not website:
            return None

        # Extract domain from website URL
        domain = website.replace("https://", "").replace("http://", "").split("/")[0]
        if not domain:
            return None

        # Try multiple logo services in order of quality
        # 1. Google Favicons (most reliable, good quality)
        google_logo = f"https://www.google.com/s2/favicons?sz=128&domain={domain}"

        # 2. Unavatar (good fallback)
        _unavatar_logo = f"https://unavatar.io/{domain}"

        # 3. DuckDuckGo (lower quality but reliable)
        _ddg_logo = f"https://icons.duckduckgo.com/ip3/{domain}.ico"

        # Return Google Favicons by default (most reliable)
        # The frontend can try fallbacks if needed
        return google_logo

    except Exception:
        pass
    return None


def get_top_500_stocks_manifest():
    """Returns a deterministic Top 500 stock manifest (S&P 500) with logo URLs."""
    print("🌍 Building Top 500 stocks manifest (S&P 500)...")
    manifest = []
    try:
        # Wikipedia blocks default pandas User-Agent, so we need to set a custom one
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

        # Fetch the HTML with proper headers
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        # Parse the table from the HTML content (use StringIO for pandas 3.x compatibility)
        tables = pd.read_html(StringIO(response.text))
        table = tables[0]

        symbols = []
        for _, row in table.iterrows():
            symbol = str(row.get("Symbol", "")).strip()
            name = str(row.get("Security", symbol)).strip()
            if not symbol:
                continue
            # Yahoo uses '-' instead of '.' for some US tickers (e.g., BRK.B -> BRK-B)
            yahoo_symbol = symbol.replace(".", "-")
            symbols.append({"symbol": yahoo_symbol, "name": name})

        # Fetch logo URLs in bulk
        total = len(symbols)
        print(f"  → Fetching logo URLs for {total} stocks...")
        for idx, item in enumerate(symbols):
            logo_url = fetch_logo_url(item["symbol"])
            manifest.append(
                {
                    "symbol": item["symbol"],
                    "name": item["name"],
                    "logo_url": logo_url,
                }
            )
            if (idx + 1) % 50 == 0:
                print(f"    ✓ Logos: {idx + 1}/{total}")

        print(f"📊 Total top stocks manifest: {len(manifest)}")
    except Exception as e:
        print(f"✗ Error building top stocks manifest: {e}")
    return manifest


def _finnhub_quote(symbol):
    """Fallback using Finnhub API for US stocks."""
    api_key = os.getenv("FINNHUB_API_KEY")
    base_url = os.getenv("FINNHUB_API_URL", "https://finnhub.io/api/v1")

    if not api_key:
        return None, None

    # Finnhub doesn't support Yahoo-style suffixes, skip non-US symbols
    if any(x in symbol for x in [".PS", ".L", ".DE", ".PA", ".T", ".HK", "^"]):
        return None, None

    try:
        url = f"{base_url}/quote?symbol={symbol}&token={api_key}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        current = _safe_float(data.get("c"))
        prev_close = _safe_float(data.get("pc"))

        if current is None or current == 0:
            return None, None

        change_percent = 0.0
        if prev_close is not None and prev_close != 0:
            change_percent = ((current - prev_close) / prev_close) * 100

        return round(current, 2), round(change_percent, 2)
    except Exception:
        return None, None


def _compute_annual_returns(close_series):
    """
    Given a pandas Series of monthly close prices (sorted oldest→newest),
    returns (return_1y_percent, return_3y_percent, return_5y_percent) as annualized CAGR.
    Uses actual date spans for accuracy. Returns None when not enough history.
    """
    if close_series is None or len(close_series) < 2:
        return None, None, None

    latest = _safe_float(close_series.iloc[-1])
    if latest is None or latest <= 0:
        return None, None, None

    def _cagr(start_idx):
        price = _safe_float(close_series.iloc[start_idx])
        if price is None or price <= 0:
            return None
        years = (close_series.index[-1] - close_series.index[start_idx]).days / 365.25
        if years < 0.5:
            return None
        return round(((latest / price) ** (1 / years) - 1) * 100, 2)

    r1y = _cagr(-13) if len(close_series) >= 13 else None
    r3y = _cagr(-37) if len(close_series) >= 37 else None
    # For 5y, use the oldest available row in the 5y window (requires >= 55 months)
    r5y = _cagr(0) if len(close_series) >= 55 else None

    return r1y, r3y, r5y


def _fallback_quote(symbol):
    """Try single-symbol fallback via yfinance.Ticker for better reliability.
    Returns (price, change_percent, change_7d_percent, change_30d_percent,
             return_1y_percent, return_3y_percent, return_5y_percent)."""
    try:
        t = yf.Ticker(symbol)
        fi = getattr(t, "fast_info", None) or {}
        price = _safe_float(fi.get("lastPrice")) or _safe_float(fi.get("regularMarketPrice"))
        prev_close = _safe_float(fi.get("previousClose"))

        # Get 1 month of history for 7d/30d calculations
        hist = t.history(period="1mo", interval="1d")
        change_7d = None
        change_30d = None

        if hist is not None and not hist.empty:
            close_series = hist.get("Close")
            if close_series is not None and len(close_series) > 0:
                close_series = close_series.dropna()
                if price is None and len(close_series) > 0:
                    price = _safe_float(close_series.iloc[-1])
                if prev_close is None and len(close_series) >= 2:
                    prev_close = _safe_float(close_series.iloc[-2])

                # 7-day change (~5 trading days back)
                if len(close_series) >= 6 and price is not None:
                    price_7d_ago = _safe_float(close_series.iloc[-6])
                    if price_7d_ago and price_7d_ago != 0:
                        change_7d = round(((price - price_7d_ago) / price_7d_ago) * 100, 2)

                # 30-day change (first data point in 1mo range)
                if len(close_series) >= 2 and price is not None:
                    price_30d_ago = _safe_float(close_series.iloc[0])
                    if price_30d_ago and price_30d_ago != 0:
                        change_30d = round(((price - price_30d_ago) / price_30d_ago) * 100, 2)

        # Get 5-year monthly history for annual return (CAGR) calculations
        r1y, r3y, r5y = None, None, None
        try:
            hist_5y = t.history(period="5y", interval="1mo")
            if hist_5y is not None and not hist_5y.empty:
                close_5y = hist_5y.get("Close")
                if close_5y is not None:
                    r1y, r3y, r5y = _compute_annual_returns(close_5y.dropna())
        except Exception:
            pass

        if price is None:
            return None, None, None, None, None, None, None

        change_percent = 0.0
        if prev_close is not None and prev_close != 0:
            change_percent = ((price - prev_close) / prev_close) * 100

        return (
            round(price, 2),
            round(change_percent, 2),
            change_7d,
            change_30d,
            r1y,
            r3y,
            r5y,
        )
    except Exception:
        return None, None, None, None, None, None, None


def fetch_crypto_data():
    """Fetches Top 500 Crypto, creates a manifest AND a price file."""
    print("\n💰 Fetching Top 500 Crypto from CoinGecko...")
    manifest = []
    prices = []

    api_key = os.getenv("COINGECKO_API_KEY")
    base_url = os.getenv("COINGECKO_API_URL", "https://api.coingecko.com/api/v3")
    headers = {"accept": "application/json"}
    if api_key:
        headers["x-cg-demo-api-key"] = api_key

    # Fetch 2 pages of 250 = 500 total
    for page in [1, 2]:
        url = (
            f"{base_url}/coins/markets?vs_currency=usd&order=market_cap_desc"
            f"&per_page=250&page={page}&sparkline=false&price_change_percentage=7d,30d,1y"
        )
        try:
            print(f"  → Page {page}/2...")
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()

            for coin in data:
                symbol = coin["symbol"].upper()
                logo_url = coin.get("image")  # CoinGecko provides logo
                manifest.append(
                    {
                        "symbol": symbol,
                        "name": coin["name"],
                        "id": coin["id"],  # CoinGecko ID for future API calls
                        "logo_url": logo_url,
                    }
                )
                prices.append(
                    {
                        "symbol": symbol,
                        "price": coin["current_price"],
                        "change_24h_percent": round(
                            coin.get("price_change_percentage_24h") or 0, 2
                        ),
                        "change_7d_percent": round(
                            coin.get("price_change_percentage_7d_in_currency") or 0,
                            2,
                        ),
                        "change_30d_percent": round(
                            coin.get("price_change_percentage_30d_in_currency") or 0,
                            2,
                        ),
                        "return_1y_percent": round(
                            coin.get("price_change_percentage_1y_in_currency") or 0,
                            2,
                        ),
                        "market_cap": coin.get("market_cap", 0),
                        "volume_24h": coin.get("total_volume", 0),
                        "logo_url": logo_url,
                    }
                )

            print(f"    ✓ Added {len(data)} coins")
            time.sleep(2)  # Rate limit protection
        except Exception as e:
            print(f"    ✗ Error fetching crypto page {page}: {e}")

    save_json({"last_updated_utc": _utc_now_iso(), "tickers": manifest}, "crypto_manifest.json")
    save_json({"last_updated_utc": _utc_now_iso(), "prices": prices}, "crypto_prices.json")
    return manifest


def fetch_yahoo_prices(manifest, filename, batch_size=50):
    """
    Fetches prices from Yahoo for a given manifest of tickers.
    Uses smaller batches and better error handling.
    Carries logo_url from manifest into price entries.
    """
    tickers_list = [item["symbol"] for item in manifest]
    logo_lookup = {item["symbol"]: item.get("logo_url") for item in manifest}
    print(f"\n📈 Fetching prices for {len(tickers_list)} assets ({filename})...")
    prices = []
    failed = []

    total_batches = max(1, math.ceil(len(tickers_list) / batch_size))

    for i in range(0, len(tickers_list), batch_size):
        batch = tickers_list[i : i + batch_size]
        print(
            f"  → Batch {i // batch_size + 1}/{total_batches} ({len(batch)} tickers)..."
        )

        try:
            # Download 1 month of daily data for price/day/7d/30d changes
            data = yf.download(
                batch,
                period="1mo",
                interval="1d",
                group_by="ticker",
                threads=True,
                progress=False,
            )

            # Download 5-year monthly data for annual return (CAGR) calculations
            cagr_lookup = {sym: (None, None, None) for sym in batch}
            try:
                data_5y = yf.download(
                    batch,
                    period="5y",
                    interval="1mo",
                    group_by="ticker",
                    threads=True,
                    progress=False,
                )
                if data_5y is not None and not data_5y.empty:
                    for sym in batch:
                        try:
                            df5 = data_5y if len(batch) == 1 else data_5y[sym]
                            if df5 is not None and not df5.empty:
                                c5 = df5["Close"].dropna()
                                cagr_lookup[sym] = _compute_annual_returns(c5)
                        except Exception:
                            pass
            except Exception:
                pass

            for symbol in batch:
                try:
                    close_price = None
                    open_price = None
                    change_7d = None
                    change_30d = None

                    # Handle single ticker vs multi-ticker dataframe structure
                    if len(batch) == 1:
                        df = data
                    else:
                        try:
                            df = data[symbol]
                        except Exception:
                            df = pd.DataFrame()

                    if df is not None and not df.empty and len(df) > 0:
                        close_series = df.get("Close")
                        open_series = df.get("Open")
                        # Drop NaN rows (can occur in batch downloads with mixed trading days)
                        if close_series is not None:
                            close_series = close_series.dropna()
                        if open_series is not None:
                            open_series = open_series.dropna()

                        if close_series is not None and len(close_series) > 0:
                            close_price = _safe_float(close_series.iloc[-1])

                            # 7-day change (~5 trading days back)
                            if len(close_series) >= 6:
                                price_7d_ago = _safe_float(close_series.iloc[-6])
                                if price_7d_ago and price_7d_ago != 0:
                                    change_7d = round(
                                        ((close_price - price_7d_ago) / price_7d_ago) * 100,
                                        2,
                                    )

                            # 30-day change (first available data point in 1mo range)
                            if len(close_series) >= 2:
                                price_30d_ago = _safe_float(close_series.iloc[0])
                                if price_30d_ago and price_30d_ago != 0:
                                    change_30d = round(
                                        ((close_price - price_30d_ago) / price_30d_ago) * 100,
                                        2,
                                    )

                        if open_series is not None and len(open_series) > 0:
                            open_price = _safe_float(open_series.iloc[-1])

                    # Fallback if batch download didn't give a valid price
                    if close_price is None:
                        # Try yfinance single-ticker fallback first
                        (
                            close_price,
                            fallback_change,
                            fb_7d,
                            fb_30d,
                            fb_1y,
                            fb_3y,
                            fb_5y,
                        ) = _fallback_quote(symbol)

                        # If yfinance also failed, try Finnhub as last resort
                        if close_price is None:
                            close_price, fallback_change = _finnhub_quote(symbol)
                            fb_7d, fb_30d, fb_1y, fb_3y, fb_5y = (
                                None,
                                None,
                                None,
                                None,
                                None,
                            )

                        if close_price is None:
                            failed.append(symbol)
                            continue

                        prices.append(
                            {
                                "symbol": symbol,
                                "price": close_price,
                                "change_percent": fallback_change
                                if fallback_change is not None
                                else 0.0,
                                "change_7d_percent": fb_7d,
                                "change_30d_percent": fb_30d,
                                "return_1y_percent": fb_1y,
                                "return_3y_percent": fb_3y,
                                "return_5y_percent": fb_5y,
                                "logo_url": logo_lookup.get(symbol),
                            }
                        )
                        continue

                    # Calculate percent change from open when available
                    if open_price is not None and open_price != 0:
                        change_percent = ((close_price - open_price) / open_price) * 100
                    else:
                        _, fallback_change, _, _, _, _, _ = _fallback_quote(symbol)
                        change_percent = fallback_change if fallback_change is not None else 0.0

                    r1y, r3y, r5y = cagr_lookup.get(symbol, (None, None, None))
                    prices.append(
                        {
                            "symbol": symbol,
                            "price": round(close_price, 2),
                            "change_percent": round(float(change_percent), 2),
                            "change_7d_percent": change_7d,
                            "change_30d_percent": change_30d,
                            "return_1y_percent": r1y,
                            "return_3y_percent": r3y,
                            "return_5y_percent": r5y,
                            "logo_url": logo_lookup.get(symbol),
                        }
                    )

                except Exception:
                    failed.append(symbol)
                    continue

            time.sleep(1)  # Rate limit protection

        except Exception as e:
            print(f"    ✗ Batch error: {e}")
            failed.extend(batch)
            continue

    print(f"  ✅ Success: {len(prices)}/{len(tickers_list)} | ❌ Failed: {len(failed)}")
    if failed and len(failed) < 20:  # Only print if not too many
        print(
            f"  Failed tickers: {', '.join(failed[:10])}{'...' if len(failed) > 10 else ''}"
        )

    save_json(
        {
            "last_updated_utc": _utc_now_iso(),
            "prices": prices,
            "failed_tickers": failed,
        },
        filename,
    )


def fetch_yahoo_daily_history(manifest, filename, days=DAILY_HISTORY_DAYS, batch_size=50):
    """
    Builds per-ticker daily close history from Yahoo.
    Output shape:
    {
      last_updated_utc,
      timeframe_days,
      histories: [{symbol, points:[{date, close}]}],
      failed_tickers
    }
    """
    tickers_list = [item["symbol"] for item in manifest]
    histories = []
    failed = []

    print(
        f"\n📉 Fetching Yahoo daily history ({days}d) for {len(tickers_list)} assets ({filename})..."
    )

    total_batches = max(1, math.ceil(len(tickers_list) / batch_size))

    for i in range(0, len(tickers_list), batch_size):
        batch = tickers_list[i : i + batch_size]
        print(
            f"  → History batch {i // batch_size + 1}/{total_batches} ({len(batch)} tickers)..."
        )

        try:
            data = yf.download(
                batch,
                period=DAILY_HISTORY_PERIOD,
                interval="1d",
                group_by="ticker",
                threads=True,
                progress=False,
            )

            for symbol in batch:
                points = []

                try:
                    if len(batch) == 1:
                        df = data
                    else:
                        try:
                            df = data[symbol]
                        except Exception:
                            df = pd.DataFrame()

                    if df is not None and not df.empty:
                        points = _series_to_daily_points(df.get("Close"), days=days)

                    if len(points) < 2:
                        points = _fallback_daily_points(symbol, days=days)

                    if len(points) < 2:
                        failed.append(symbol)
                        continue

                    histories.append({"symbol": symbol, "points": points})

                except Exception:
                    failed.append(symbol)
                    continue

            time.sleep(1)

        except Exception as e:
            print(f"    ✗ History batch error: {e}")
            failed.extend(batch)
            continue

    print(f"  ✅ History success: {len(histories)}/{len(tickers_list)} | ❌ Failed: {len(failed)}")

    save_json(
        {
            "last_updated_utc": _utc_now_iso(),
            "timeframe_days": days,
            "histories": histories,
            "failed_tickers": failed,
        },
        filename,
    )


def _coingecko_market_chart_points(coin_id, days=DAILY_HISTORY_DAYS):
    """Fetch one coin's daily price history from CoinGecko."""
    api_key = os.getenv("COINGECKO_API_KEY")
    base_url = os.getenv("COINGECKO_API_URL", "https://api.coingecko.com/api/v3")
    headers = {"accept": "application/json"}
    if api_key:
        headers["x-cg-demo-api-key"] = api_key

    try:
        url = f"{base_url}/coins/{coin_id}/market_chart?vs_currency=usd&days={days}&interval=daily"
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        payload = response.json()
        raw_prices = payload.get("prices") or []

        points = []
        for item in raw_prices:
            if not isinstance(item, list) or len(item) < 2:
                continue

            ts = item[0]
            close = _safe_float(item[1])
            if close is None:
                continue

            try:
                date_str = pd.to_datetime(ts, unit="ms", utc=True).strftime("%Y-%m-%d")
            except Exception:
                continue

            points.append({"date": date_str, "close": round(close, 6)})

        points.sort(key=lambda x: x["date"])
        if len(points) > days:
            points = points[-days:]

        return points
    except Exception:
        return []


def fetch_crypto_daily_history(
    crypto_manifest,
    filename,
    days=DAILY_HISTORY_DAYS,
    batch_size=80,
    coingecko_fallback_limit=80,
):
    """
    Builds crypto daily history in two passes:
    1) Yahoo batch download via SYMBOL-USD (fast, broad)
    2) CoinGecko market_chart fallback for misses (rate-limited)
    """
    print(
        f"\n🪙 Fetching crypto daily history ({days}d) for {len(crypto_manifest)} assets ({filename})..."
    )

    symbol_to_id = {c["symbol"].upper(): c.get("id") for c in crypto_manifest}
    yahoo_to_symbol = {f"{c['symbol'].upper()}-USD": c["symbol"].upper() for c in crypto_manifest}
    yahoo_symbols = list(yahoo_to_symbol.keys())

    histories_map = {}
    failed = []

    total_batches = max(1, math.ceil(len(yahoo_symbols) / batch_size))

    for i in range(0, len(yahoo_symbols), batch_size):
        batch = yahoo_symbols[i : i + batch_size]
        print(
            f"  → Crypto history batch {i // batch_size + 1}/{total_batches} ({len(batch)} tickers)..."
        )

        try:
            data = yf.download(
                batch,
                period=DAILY_HISTORY_PERIOD,
                interval="1d",
                group_by="ticker",
                threads=True,
                progress=False,
            )

            for yahoo_symbol in batch:
                original_symbol = yahoo_to_symbol[yahoo_symbol]
                points = []

                try:
                    if len(batch) == 1:
                        df = data
                    else:
                        try:
                            df = data[yahoo_symbol]
                        except Exception:
                            df = pd.DataFrame()

                    if df is not None and not df.empty:
                        points = _series_to_daily_points(df.get("Close"), days=days)

                    if len(points) >= 2:
                        histories_map[original_symbol] = points
                except Exception:
                    continue

            time.sleep(1)

        except Exception as e:
            print(f"    ✗ Crypto history batch error: {e}")
            continue

    # CoinGecko fallback for missing histories
    missing_symbols = [
        c["symbol"].upper()
        for c in crypto_manifest
        if c["symbol"].upper() not in histories_map
    ]

    fallback_targets = missing_symbols[:coingecko_fallback_limit]
    if fallback_targets:
        print(
            f"  → CoinGecko fallback for {len(fallback_targets)} missing assets "
            f"(capped at {coingecko_fallback_limit})..."
        )

    for idx, symbol in enumerate(fallback_targets):
        coin_id = symbol_to_id.get(symbol)
        if not coin_id:
            failed.append(symbol)
            continue

        points = _coingecko_market_chart_points(coin_id, days=days)
        if len(points) >= 2:
            histories_map[symbol] = points
        else:
            failed.append(symbol)

        if (idx + 1) % 20 == 0:
            print(f"    ✓ CoinGecko fallback progress: {idx + 1}/{len(fallback_targets)}")
        time.sleep(1.2)

    # Mark remaining misses beyond fallback cap as failed
    uncaptured_symbols = [
        c["symbol"].upper()
        for c in crypto_manifest
        if c["symbol"].upper() not in histories_map and c["symbol"].upper() not in failed
    ]
    failed.extend(uncaptured_symbols)

    histories = [
        {"symbol": symbol, "points": points}
        for symbol, points in sorted(histories_map.items(), key=lambda x: x[0])
    ]

    print(
        f"  ✅ Crypto history success: {len(histories)}/{len(crypto_manifest)} | ❌ Failed: {len(failed)}"
    )

    save_json(
        {
            "last_updated_utc": _utc_now_iso(),
            "timeframe_days": days,
            "histories": histories,
            "failed_tickers": failed,
        },
        filename,
    )


def main():
    print("=" * 60)
    print("🚀 MARKET DATA FETCHER")
    print("=" * 60)

    # 1. Crypto (Top 500)
    crypto_manifest = fetch_crypto_data()

    # 1b. Crypto daily history (30d)
    fetch_crypto_daily_history(
        crypto_manifest,
        "crypto_daily_30d.json",
        days=DAILY_HISTORY_DAYS,
        batch_size=80,
        coingecko_fallback_limit=80,
    )

    # 2. Top 500 Stocks (S&P 500)
    print("\n" + "=" * 60)
    top_500_manifest = get_top_500_stocks_manifest()

    # 3. Add Philippine Stocks (optional local coverage)
    print("\n🇵🇭 Adding Philippine Stocks...")
    print("  → Fetching logo URLs for PH stocks...")
    ph_with_logos = []
    for stock in PH_STOCKS:
        logo_url = fetch_logo_url(stock["symbol"])
        ph_with_logos.append({**stock, "logo_url": logo_url})

    all_stocks_manifest = top_500_manifest + ph_with_logos
    print(f"  ✓ Added {len(PH_STOCKS)} PH stocks")
    print(f"📊 Total stocks manifest: {len(all_stocks_manifest)}")

    save_json(
        {"last_updated_utc": _utc_now_iso(), "tickers": all_stocks_manifest},
        "stocks_manifest.json",
    )

    # 4. Fetch Stock Prices
    fetch_yahoo_prices(all_stocks_manifest, "stocks_prices.json", batch_size=50)

    # 4b. Stocks daily history (30d)
    fetch_yahoo_daily_history(
        all_stocks_manifest,
        "stocks_daily_30d.json",
        days=DAILY_HISTORY_DAYS,
        batch_size=40,
    )

    # 5. Indices only
    print("\n" + "=" * 60)
    print("📊 Processing Indices...")
    print("  → Fetching logo URLs for indices...")
    indices_with_logos = []
    for item in INDICES:
        logo_url = fetch_logo_url(item["symbol"])
        indices_with_logos.append({**item, "logo_url": logo_url})

    save_json(
        {"last_updated_utc": _utc_now_iso(), "tickers": indices_with_logos},
        "indices_commodities_manifest.json",
    )

    fetch_yahoo_prices(indices_with_logos, "indices_commodities_prices.json", batch_size=6)

    # 5b. Indices daily history (30d)
    fetch_yahoo_daily_history(
        indices_with_logos,
        "indices_daily_30d.json",
        days=DAILY_HISTORY_DAYS,
        batch_size=6,
    )

    # 6. Metals & Commodities
    print("\n" + "=" * 60)
    print("🥇 Processing Metals & Commodities...")
    # Metals already have logo URLs defined in the constant (from GitHub)

    save_json(
        {"last_updated_utc": _utc_now_iso(), "tickers": METALS_COMMODITIES},
        "metals_indices_manifest.json",
    )

    fetch_yahoo_prices(METALS_COMMODITIES, "metals_indices_prices.json", batch_size=8)

    # 6b. Metals daily history (30d)
    fetch_yahoo_daily_history(
        METALS_COMMODITIES,
        "metals_daily_30d.json",
        days=DAILY_HISTORY_DAYS,
        batch_size=8,
    )

    print("\n" + "=" * 60)
    print("✅ COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
