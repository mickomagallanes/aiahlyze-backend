import yfinance as yf
import pandas as pd
import requests
import json
import time
import os
from datetime import datetime, timezone
import math
from io import StringIO

# --- CONFIGURATION ---

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
    {"symbol": "MNDDF", "name": "Monde Nissin Corporation (OTC)"}
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
    {"symbol": "^BVSP", "name": "Bovespa Index (Brazil)"}
]

# Metals and Commodities (separate tracking)
# Logo URLs point to uploaded images on GitHub
METALS_COMMODITIES = [
    {"symbol": "GC=F", "name": "Gold", "logo_url": "https://raw.githubusercontent.com/mickomagallanes/aiahlyze-backend/refs/heads/main/gold.png"},
    {"symbol": "SI=F", "name": "Silver", "logo_url": "https://raw.githubusercontent.com/mickomagallanes/aiahlyze-backend/refs/heads/main/silver.png"},
    {"symbol": "CL=F", "name": "Crude Oil (WTI)", "logo_url": "https://raw.githubusercontent.com/mickomagallanes/aiahlyze-backend/refs/heads/main/oil.png"},
    {"symbol": "BZ=F", "name": "Brent Crude Oil", "logo_url": "https://raw.githubusercontent.com/mickomagallanes/aiahlyze-backend/refs/heads/main/brent.png"},
    {"symbol": "NG=F", "name": "Natural Gas", "logo_url": "https://raw.githubusercontent.com/mickomagallanes/aiahlyze-backend/refs/heads/main/natural-gas.png"},
    {"symbol": "HG=F", "name": "Copper", "logo_url": "https://raw.githubusercontent.com/mickomagallanes/aiahlyze-backend/refs/heads/main/copper.png"},
    {"symbol": "PL=F", "name": "Platinum", "logo_url": "https://raw.githubusercontent.com/mickomagallanes/aiahlyze-backend/refs/heads/main/platinum.png"},
    {"symbol": "PA=F", "name": "Palladium", "logo_url": "https://raw.githubusercontent.com/mickomagallanes/aiahlyze-backend/refs/heads/main/palladium.png"}
]

def save_json(data, filename):
    """Helper to save a dictionary to a JSON file."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✅ Successfully saved {len(data.get('prices', data.get('tickers', [])))} items to {filename}")


def fetch_logo_url(symbol):
    """
    Try to get a logo URL for a ticker via yfinance website field.
    Uses multiple fallback services: Google Favicons, Unavatar, DuckDuckGo.
    Returns the first working logo URL, or None if all fail.
    """
    try:
        info = yf.Ticker(symbol).info or {}
        website = info.get('website') or ''
        if not website:
            return None
            
        # Extract domain from website URL
        domain = website.replace('https://', '').replace('http://', '').split('/')[0]
        if not domain:
            return None
        
        # Try multiple logo services in order of quality
        # 1. Google Favicons (most reliable, good quality)
        google_logo = f"https://www.google.com/s2/favicons?sz=128&domain={domain}"
        
        # 2. Unavatar (good fallback)
        unavatar_logo = f"https://unavatar.io/{domain}"
        
        # 3. DuckDuckGo (lower quality but reliable)
        ddg_logo = f"https://icons.duckduckgo.com/ip3/{domain}.ico"
        
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
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Fetch the HTML with proper headers
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        # Parse the table from the HTML content (use StringIO for pandas 3.x compatibility)
        tables = pd.read_html(StringIO(response.text))
        table = tables[0]
        
        symbols = []
        for _, row in table.iterrows():
            symbol = str(row.get('Symbol', '')).strip()
            name = str(row.get('Security', symbol)).strip()
            if not symbol:
                continue
            # Yahoo uses '-' instead of '.' for some US tickers (e.g., BRK.B -> BRK-B)
            yahoo_symbol = symbol.replace('.', '-')
            symbols.append({"symbol": yahoo_symbol, "name": name})
        
        # Fetch logo URLs in bulk
        total = len(symbols)
        print(f"  → Fetching logo URLs for {total} stocks...")
        for idx, item in enumerate(symbols):
            logo_url = fetch_logo_url(item['symbol'])
            manifest.append({
                "symbol": item['symbol'],
                "name": item['name'],
                "logo_url": logo_url
            })
            if (idx + 1) % 50 == 0:
                print(f"    ✓ Logos: {idx + 1}/{total}")
        
        print(f"📊 Total top stocks manifest: {len(manifest)}")
    except Exception as e:
        print(f"✗ Error building top stocks manifest: {e}")
    return manifest


def _safe_float(value):
    """Return a float if value is finite, otherwise None."""
    try:
        val = float(value)
        if math.isnan(val) or math.isinf(val):
            return None
        return val
    except Exception:
        return None


def _finnhub_quote(symbol):
    """Fallback using Finnhub API for US stocks."""
    api_key = os.getenv("FINNHUB_API_KEY")
    base_url = os.getenv("FINNHUB_API_URL", "https://finnhub.io/api/v1")
    
    if not api_key:
        return None, None
    
    # Finnhub doesn't support Yahoo-style suffixes, skip non-US symbols
    if any(x in symbol for x in ['.PS', '.L', '.DE', '.PA', '.T', '.HK', '^']):
        return None, None
    
    try:
        url = f"{base_url}/quote?symbol={symbol}&token={api_key}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        current = _safe_float(data.get('c'))
        prev_close = _safe_float(data.get('pc'))
        
        if current is None or current == 0:
            return None, None
        
        change_percent = 0.0
        if prev_close is not None and prev_close != 0:
            change_percent = ((current - prev_close) / prev_close) * 100
        
        return round(current, 2), round(change_percent, 2)
    except Exception:
        return None, None


def _fallback_quote(symbol):
    """Try single-symbol fallback via yfinance.Ticker for better reliability."""
    try:
        t = yf.Ticker(symbol)
        fi = getattr(t, 'fast_info', None) or {}
        price = _safe_float(fi.get('lastPrice')) or _safe_float(fi.get('regularMarketPrice'))
        prev_close = _safe_float(fi.get('previousClose'))

        if price is None:
            hist = t.history(period='5d', interval='1d')
            if hist is not None and not hist.empty:
                close_series = hist.get('Close')
                if close_series is not None and len(close_series) > 0:
                    price = _safe_float(close_series.iloc[-1])
                if len(close_series) >= 2:
                    prev_close = _safe_float(close_series.iloc[-2])

        if price is None:
            return None, None

        change_percent = 0.0
        if prev_close is not None and prev_close != 0:
            change_percent = ((price - prev_close) / prev_close) * 100

        return round(price, 2), round(change_percent, 2)
    except Exception:
        return None, None

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
        url = f"{base_url}/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page={page}&sparkline=false"
        try:
            print(f"  → Page {page}/2...")
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            for coin in data:
                symbol = coin['symbol'].upper()
                logo_url = coin.get('image')  # CoinGecko provides logo
                manifest.append({
                    "symbol": symbol,
                    "name": coin['name'],
                    "id": coin['id'],  # CoinGecko ID for future API calls
                    "logo_url": logo_url
                })
                prices.append({
                    "symbol": symbol,
                    "price": coin['current_price'],
                    "change_24h_percent": round(coin.get('price_change_percentage_24h', 0), 2),
                    "market_cap": coin.get('market_cap', 0),
                    "volume_24h": coin.get('total_volume', 0),
                    "logo_url": logo_url
                })
            
            print(f"    ✓ Added {len(data)} coins")
            time.sleep(2)  # Rate limit protection
        except Exception as e:
            print(f"    ✗ Error fetching crypto page {page}: {e}")
    
    save_json({"last_updated_utc": datetime.utcnow().isoformat(), "tickers": manifest}, "crypto_manifest.json")
    save_json({"last_updated_utc": datetime.utcnow().isoformat(), "prices": prices}, "crypto_prices.json")

def fetch_yahoo_prices(manifest, filename, batch_size=50):
    """
    Fetches prices from Yahoo for a given manifest of tickers.
    Uses smaller batches and better error handling.
    Carries logo_url from manifest into price entries.
    """
    tickers_list = [item['symbol'] for item in manifest]
    logo_lookup = {item['symbol']: item.get('logo_url') for item in manifest}
    print(f"\n📈 Fetching prices for {len(tickers_list)} assets ({filename})...")
    prices = []
    failed = []
    
    for i in range(0, len(tickers_list), batch_size):
        batch = tickers_list[i:i+batch_size]
        print(f"  → Batch {i//batch_size + 1}/{(len(tickers_list)//batch_size) + 1} ({len(batch)} tickers)...")
        
        try:
            # Download data for the entire batch
            data = yf.download(batch, period="1d", group_by='ticker', threads=True, progress=False)
            
            for symbol in batch:
                try:
                    close_price = None
                    open_price = None

                    # Handle single ticker vs multi-ticker dataframe structure
                    if len(batch) == 1:
                        df = data
                    else:
                        try:
                            df = data[symbol]
                        except Exception:
                            df = pd.DataFrame()

                    if df is not None and not df.empty and len(df) > 0:
                        close_series = df.get('Close')
                        open_series = df.get('Open')
                        if close_series is not None and len(close_series) > 0:
                            close_price = _safe_float(close_series.iloc[-1])
                        if open_series is not None and len(open_series) > 0:
                            open_price = _safe_float(open_series.iloc[-1])

                    # Fallback if batch download didn't give a valid price
                    if close_price is None:
                        # Try yfinance single-ticker fallback first
                        close_price, fallback_change = _fallback_quote(symbol)
                        
                        # If yfinance also failed, try Finnhub as last resort
                        if close_price is None:
                            close_price, fallback_change = _finnhub_quote(symbol)
                        
                        if close_price is None:
                            failed.append(symbol)
                            continue
                        
                        prices.append({
                            "symbol": symbol,
                            "price": close_price,
                            "change_percent": fallback_change if fallback_change is not None else 0.0,
                            "logo_url": logo_lookup.get(symbol)
                        })
                        continue

                    # Calculate percent change from open when available
                    if open_price is not None and open_price != 0:
                        change_percent = ((close_price - open_price) / open_price) * 100
                    else:
                        _, fallback_change = _fallback_quote(symbol)
                        change_percent = fallback_change if fallback_change is not None else 0.0

                    prices.append({
                        "symbol": symbol,
                        "price": round(close_price, 2),
                        "change_percent": round(float(change_percent), 2),
                        "logo_url": logo_lookup.get(symbol)
                    })
                    
                except Exception as e:
                    failed.append(symbol)
                    continue
            
            time.sleep(1)  # Rate limit protection
            
        except Exception as e:
            print(f"    ✗ Batch error: {e}")
            failed.extend(batch)
            continue
    
    print(f"  ✅ Success: {len(prices)}/{len(tickers_list)} | ❌ Failed: {len(failed)}")
    if failed and len(failed) < 20:  # Only print if not too many
        print(f"  Failed tickers: {', '.join(failed[:10])}{'...' if len(failed) > 10 else ''}")
    
    save_json({
        "last_updated_utc": datetime.utcnow().isoformat(),
        "prices": prices,
        "failed_tickers": failed
    }, filename)

def main():
    print("=" * 60)
    print("🚀 MARKET DATA FETCHER")
    print("=" * 60)
    
    # 1. Crypto (Top 500)
    fetch_crypto_data()
    
    # 2. Top 500 Stocks (S&P 500)
    print("\n" + "=" * 60)
    top_500_manifest = get_top_500_stocks_manifest()
    
    # 3. Add Philippine Stocks (optional local coverage)
    print("\n🇵🇭 Adding Philippine Stocks...")
    print("  → Fetching logo URLs for PH stocks...")
    ph_with_logos = []
    for stock in PH_STOCKS:
        logo_url = fetch_logo_url(stock['symbol'])
        ph_with_logos.append({**stock, "logo_url": logo_url})
    
    all_stocks_manifest = top_500_manifest + ph_with_logos
    print(f"  ✓ Added {len(PH_STOCKS)} PH stocks")
    print(f"📊 Total stocks manifest: {len(all_stocks_manifest)}")
    
    save_json({
        "last_updated_utc": datetime.utcnow().isoformat(),
        "tickers": all_stocks_manifest
    }, "stocks_manifest.json")
    
    # 4. Fetch Stock Prices
    fetch_yahoo_prices(all_stocks_manifest, "stocks_prices.json", batch_size=50)
    
    # 5. Indices only
    print("\n" + "=" * 60)
    print("📊 Processing Indices...")
    print("  → Fetching logo URLs for indices...")
    indices_with_logos = []
    for item in INDICES:
        logo_url = fetch_logo_url(item['symbol'])
        indices_with_logos.append({**item, "logo_url": logo_url})
    
    save_json({
        "last_updated_utc": datetime.utcnow().isoformat(),
        "tickers": indices_with_logos
    }, "indices_commodities_manifest.json")
    
    fetch_yahoo_prices(indices_with_logos, "indices_commodities_prices.json", batch_size=6)
    
    # 6. Metals & Commodities
    print("\n" + "=" * 60)
    print("🥇 Processing Metals & Commodities...")
    # Metals already have logo URLs defined in the constant (from GitHub)
    
    save_json({
        "last_updated_utc": datetime.utcnow().isoformat(),
        "tickers": METALS_COMMODITIES
    }, "metals_indices_manifest.json")
    
    fetch_yahoo_prices(METALS_COMMODITIES, "metals_indices_prices.json", batch_size=8)
    
    print("\n" + "=" * 60)
    print("✅ COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
