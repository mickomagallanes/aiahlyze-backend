import yfinance as yf
import pandas as pd
import requests
import json
import time
import os
from datetime import datetime

# --- CONFIGURATION ---
PH_STOCKS = [
    "SM.PS", "BDO.PS", "ALI.PS", "AC.PS", "ICT.PS", "JFC.PS", "BPI.PS", "SMPH.PS", "TEL.PS"
]
METALS_INDICES = [
    {"symbol": "GC=F", "name": "Gold"},
    {"symbol": "SI=F", "name": "Silver"},
    {"symbol": "CL=F", "name": "Crude Oil"},
    {"symbol": "^GSPC", "name": "S&P 500 Index"},
    {"symbol": "^IXIC", "name": "NASDAQ Composite"},
    {"symbol": "^PSI", "name": "PSEi Index"}
]

def save_json(data, filename):
    """Helper to save a dictionary to a JSON file."""
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Successfully saved data to {filename}")

def get_sp500_tickers():
    """Scrapes S&P 500 manifest (symbol and name) from Wikipedia."""
    print("Scraping S&P 500 manifest...")
    try:
        table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
        # Create a list of dictionaries: [{"symbol": "MSFT", "name": "Microsoft"}, ...]
        return table[['Symbol', 'Security']].rename(columns={'Security': 'name'}).to_dict('records')
    except Exception as e:
        print(f"Error scraping S&P 500: {e}")
        return [{"symbol": "AAPL", "name": "Apple Inc."}] # Fallback

def fetch_crypto_data():
    """Fetches Top 500 Crypto, creates a manifest AND a price file."""
    print("Fetching Top 500 Crypto from CoinGecko...")
    manifest = []
    prices = []
    
    api_key = os.getenv("COINGECKO_API_KEY")
    headers = {"accept": "application/json"}
    if api_key: headers["x-cg-demo-api-key"] = api_key

    for page in [1, 2]:
        url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page={page}"
        try:
            response = requests.get(url, headers=headers)
            data = response.json()
            for coin in data:
                manifest.append({"symbol": coin['symbol'].upper(), "name": coin['name']})
                prices.append({"symbol": coin['symbol'].upper(), "price": coin['current_price'], "change_24h_percent": coin.get('price_change_percentage_24h', 0)})
            time.sleep(2)
        except Exception as e: print(f"Error fetching crypto page {page}: {e}")

    save_json({"last_updated_utc": datetime.utcnow().isoformat(), "tickers": manifest}, "crypto_manifest.json")
    save_json({"last_updated_utc": datetime.utcnow().isoformat(), "prices": prices}, "crypto_prices.json")

def fetch_yahoo_prices(manifest, filename):
    """Fetches prices from Yahoo for a given manifest of tickers."""
    tickers = [item['symbol'] for item in manifest]
    print(f"Fetching prices for {len(tickers)} assets for {filename}...")
    prices = []
    
    chunk_size = 100 # Smaller chunk size for safety
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        clean_chunk = [s.replace('.', '-') if ".PS" not in s and "^" not in s else s for s in chunk]
        try:
            data = yf.download(clean_chunk, period="1d", group_by='ticker', threads=True, progress=False)
            for symbol in chunk: # Use original symbol for matching
                clean_symbol = symbol.replace('.', '-') if ".PS" not in symbol and "^" not in symbol else symbol
                df = data[clean_symbol] if len(clean_chunk) > 1 else data
                if not df.empty:
                    price = df['Close'].iloc[-1]
                    open_p = df['Open'].iloc[-1]
                    change = ((price - open_p) / open_p) * 100 if open_p != 0 else 0
                    prices.append({"symbol": symbol, "price": round(float(price), 2), "change_percent": round(float(change), 2)})
        except Exception: continue
        time.sleep(1)
        
    save_json({"last_updated_utc": datetime.utcnow().isoformat(), "prices": prices}, filename)


def main():
    # 1. Process Crypto
    fetch_crypto_data()

    # 2. Process Stocks
    print("Processing Stocks...")
    sp500_manifest = get_sp500_tickers()
    # For PH Stocks, we create a simple manifest
    ph_manifest = [{"symbol": s, "name": s.replace('.PS', '')} for s in PH_STOCKS]
    stocks_manifest = sp500_manifest + ph_manifest
    save_json({"last_updated_utc": datetime.utcnow().isoformat(), "tickers": stocks_manifest}, "stocks_manifest.json")
    fetch_yahoo_prices(stocks_manifest, "stocks_prices.json")
    
    # 3. Process Metals & Indices
    print("Processing Metals & Indices...")
    save_json({"last_updated_utc": datetime.utcnow().isoformat(), "tickers": METALS_INDICES}, "metals_indices_manifest.json")
    fetch_yahoo_prices(METALS_INDICES, "metals_indices_prices.json")

if __name__ == "__main__":
    main()
