import yfinance as yf
import pandas as pd
import requests
import json
import time
import os
from datetime import datetime

# --- CONFIGURATION ---

# Top 30 Philippine Stocks (by market cap)
PH_STOCKS = [
    {"symbol": "SM.PS", "name": "SM Investments Corporation"},
    {"symbol": "BDO.PS", "name": "BDO Unibank"},
    {"symbol": "ALI.PS", "name": "Ayala Land"},
    {"symbol": "AC.PS", "name": "Ayala Corporation"},
    {"symbol": "ICT.PS", "name": "International Container Terminal Services"},
    {"symbol": "JFC.PS", "name": "Jollibee Foods Corporation"},
    {"symbol": "BPI.PS", "name": "Bank of the Philippine Islands"},
    {"symbol": "SMPH.PS", "name": "SM Prime Holdings"},
    {"symbol": "TEL.PS", "name": "PLDT Inc."},
    {"symbol": "MBT.PS", "name": "Metrobank"},
    {"symbol": "GLO.PS", "name": "Globe Telecom"},
    {"symbol": "URC.PS", "name": "Universal Robina Corporation"},
    {"symbol": "GTCAP.PS", "name": "GT Capital Holdings"},
    {"symbol": "DMC.PS", "name": "DMCI Holdings"},
    {"symbol": "AEV.PS", "name": "Aboitiz Equity Ventures"},
    {"symbol": "AP.PS", "name": "Aboitiz Power Corporation"},
    {"symbol": "SECB.PS", "name": "Security Bank Corporation"},
    {"symbol": "MEG.PS", "name": "Megaworld Corporation"},
    {"symbol": "PGOLD.PS", "name": "Puregold Price Club"},
    {"symbol": "RLC.PS", "name": "Robinsons Land Corporation"},
    {"symbol": "CNPF.PS", "name": "Century Pacific Food"},
    {"symbol": "MPI.PS", "name": "Metro Pacific Investments Corporation"},
    {"symbol": "BLOOM.PS", "name": "Bloomberry Resorts Corporation"},
    {"symbol": "MONDE.PS", "name": "Monde Nissin Corporation"},
    {"symbol": "LTG.PS", "name": "LT Group"},
    {"symbol": "AGI.PS", "name": "Alliance Global Group"},
    {"symbol": "SCC.PS", "name": "Semirara Mining and Power Corporation"},
    {"symbol": "PCOR.PS", "name": "Petron Corporation"},
    {"symbol": "CEI.PS", "name": "Crown Equities"},
    {"symbol": "SEVN.PS", "name": "Philippine Seven Corporation"}
]

# Major Global Indices and Commodities
INDICES_AND_COMMODITIES = [
    {"symbol": "^GSPC", "name": "S&P 500 Index"},
    {"symbol": "^IXIC", "name": "NASDAQ Composite"},
    {"symbol": "^DJI", "name": "Dow Jones Industrial Average"},
    {"symbol": "^PSI", "name": "PSEi Index (Philippines)"},
    {"symbol": "^N225", "name": "Nikkei 225 (Japan)"},
    {"symbol": "^HSI", "name": "Hang Seng Index (Hong Kong)"},
    {"symbol": "^FTSE", "name": "FTSE 100 (UK)"},
    {"symbol": "^GDAXI", "name": "DAX (Germany)"},
    {"symbol": "^FCHI", "name": "CAC 40 (France)"},
    {"symbol": "^STOXX50E", "name": "Euro Stoxx 50"},
    {"symbol": "^AXJO", "name": "ASX 200 (Australia)"},
    {"symbol": "^BVSP", "name": "Bovespa Index (Brazil)"},
    {"symbol": "GC=F", "name": "Gold Futures"},
    {"symbol": "SI=F", "name": "Silver Futures"},
    {"symbol": "CL=F", "name": "Crude Oil WTI Futures"},
    {"symbol": "BZ=F", "name": "Brent Crude Oil Futures"},
    {"symbol": "NG=F", "name": "Natural Gas Futures"},
    {"symbol": "HG=F", "name": "Copper Futures"},
    {"symbol": "PL=F", "name": "Platinum Futures"},
    {"symbol": "PA=F", "name": "Palladium Futures"}
]

def save_json(data, filename):
    """Helper to save a dictionary to a JSON file."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"✅ Successfully saved {len(data.get('prices', data.get('tickers', [])))} items to {filename}")

def get_global_stocks_manifest():
    """
    Returns a manifest of ~1000 global stocks:
    - S&P 500 (USA - top 500)
    - FTSE 100 (UK - top 100)
    - DAX 40 (Germany - top 40)
    - CAC 40 (France - top 40)
    - Nikkei 225 (Japan - top 225)
    - Hang Seng (Hong Kong - top 50)
    - ASX 200 (Australia - top 50 from 200)
    
    Total: ~1000 stocks
    """
    print("🌍 Scraping global stocks manifest...")
    manifest = []
    
    # 1. S&P 500 (USA)
    try:
        print("  → S&P 500 (USA)...")
        table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
        sp500 = table[['Symbol', 'Security']].rename(columns={'Symbol': 'symbol', 'Security': 'name'}).to_dict('records')
        manifest.extend(sp500)
        print(f"    ✓ Added {len(sp500)} stocks")
    except Exception as e:
        print(f"    ✗ Error: {e}")
    
    # 2. FTSE 100 (UK)
    try:
        print("  → FTSE 100 (UK)...")
        table = pd.read_html('https://en.wikipedia.org/wiki/FTSE_100_Index')[4]
        ftse = []
        for _, row in table.iterrows():
            if pd.notna(row.get('Ticker')):
                symbol = str(row['Ticker']).strip()
                # UK stocks on Yahoo need .L suffix
                if not symbol.endswith('.L'):
                    symbol = f"{symbol}.L"
                ftse.append({"symbol": symbol, "name": str(row.get('Company', symbol))})
        manifest.extend(ftse)
        print(f"    ✓ Added {len(ftse)} stocks")
    except Exception as e:
        print(f"    ✗ Error: {e}")
    
    # 3. DAX 40 (Germany)
    try:
        print("  → DAX 40 (Germany)...")
        table = pd.read_html('https://en.wikipedia.org/wiki/DAX')[4]
        dax = []
        for _, row in table.iterrows():
            if pd.notna(row.get('Ticker symbol')):
                symbol = str(row['Ticker symbol']).strip()
                # German stocks on Yahoo need .DE suffix
                if not symbol.endswith('.DE'):
                    symbol = f"{symbol}.DE"
                dax.append({"symbol": symbol, "name": str(row.get('Company', symbol))})
        manifest.extend(dax)
        print(f"    ✓ Added {len(dax)} stocks")
    except Exception as e:
        print(f"    ✗ Error: {e}")
    
    # 4. CAC 40 (France)
    try:
        print("  → CAC 40 (France)...")
        table = pd.read_html('https://en.wikipedia.org/wiki/CAC_40')[4]
        cac = []
        for _, row in table.iterrows():
            if pd.notna(row.get('Ticker')):
                symbol = str(row['Ticker']).strip()
                # French stocks on Yahoo need .PA suffix
                if not symbol.endswith('.PA'):
                    symbol = f"{symbol}.PA"
                cac.append({"symbol": symbol, "name": str(row.get('Company', symbol))})
        manifest.extend(cac)
        print(f"    ✓ Added {len(cac)} stocks")
    except Exception as e:
        print(f"    ✗ Error: {e}")
    
    # 5. Nikkei 225 (Japan) - Top 100 only (full 225 is too much)
    try:
        print("  → Nikkei 225 (Japan - top 100)...")
        table = pd.read_html('https://en.wikipedia.org/wiki/Nikkei_225')[3]
        nikkei = []
        count = 0
        for _, row in table.iterrows():
            if count >= 100:
                break
            if pd.notna(row.get('Code')):
                code = str(row['Code']).strip()
                # Japanese stocks on Yahoo need .T suffix
                symbol = f"{code}.T"
                nikkei.append({"symbol": symbol, "name": str(row.get('Name', symbol))})
                count += 1
        manifest.extend(nikkei)
        print(f"    ✓ Added {len(nikkei)} stocks")
    except Exception as e:
        print(f"    ✗ Error: {e}")
    
    # 6. Hang Seng (Hong Kong) - Top 50
    try:
        print("  → Hang Seng (Hong Kong - top 50)...")
        table = pd.read_html('https://en.wikipedia.org/wiki/Hang_Seng_Index')[3]
        hsi = []
        count = 0
        for _, row in table.iterrows():
            if count >= 50:
                break
            if pd.notna(row.get('Stock code')):
                code = str(row['Stock code']).strip()
                # HK stocks on Yahoo need .HK suffix, and code should be padded to 4 digits
                code_padded = code.zfill(4)
                symbol = f"{code_padded}.HK"
                hsi.append({"symbol": symbol, "name": str(row.get('Stock name', symbol))})
                count += 1
        manifest.extend(hsi)
        print(f"    ✓ Added {len(hsi)} stocks")
    except Exception as e:
        print(f"    ✗ Error: {e}")
    
    print(f"📊 Total global stocks manifest: {len(manifest)}")
    return manifest

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
                manifest.append({
                    "symbol": symbol,
                    "name": coin['name'],
                    "id": coin['id']  # CoinGecko ID for future API calls
                })
                prices.append({
                    "symbol": symbol,
                    "price": coin['current_price'],
                    "change_24h_percent": round(coin.get('price_change_percentage_24h', 0), 2),
                    "market_cap": coin.get('market_cap', 0),
                    "volume_24h": coin.get('total_volume', 0)
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
    """
    tickers_list = [item['symbol'] for item in manifest]
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
                    # Handle single ticker vs multi-ticker dataframe structure
                    if len(batch) == 1:
                        df = data
                    else:
                        df = data[symbol]
                    
                    if df.empty or len(df) == 0:
                        failed.append(symbol)
                        continue
                    
                    close_price = df['Close'].iloc[-1]
                    open_price = df['Open'].iloc[-1]
                    
                    # Calculate percent change
                    if pd.notna(open_price) and open_price != 0:
                        change_percent = ((close_price - open_price) / open_price) * 100
                    else:
                        change_percent = 0
                    
                    prices.append({
                        "symbol": symbol,
                        "price": round(float(close_price), 2),
                        "change_percent": round(float(change_percent), 2)
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
    
    # 2. Global Stocks (~1000 stocks)
    print("\n" + "=" * 60)
    global_manifest = get_global_stocks_manifest()
    
    # 3. Add Philippine Stocks
    print("\n🇵🇭 Adding Philippine Stocks...")
    all_stocks_manifest = global_manifest + PH_STOCKS
    print(f"  ✓ Added {len(PH_STOCKS)} PH stocks")
    print(f"📊 Total stocks manifest: {len(all_stocks_manifest)}")
    
    save_json({
        "last_updated_utc": datetime.utcnow().isoformat(),
        "tickers": all_stocks_manifest
    }, "stocks_manifest.json")
    
    # 4. Fetch Stock Prices
    fetch_yahoo_prices(all_stocks_manifest, "stocks_prices.json", batch_size=50)
    
    # 5. Indices and Commodities
    print("\n" + "=" * 60)
    print("📊 Processing Indices & Commodities...")
    save_json({
        "last_updated_utc": datetime.utcnow().isoformat(),
        "tickers": INDICES_AND_COMMODITIES
    }, "indices_commodities_manifest.json")
    
    fetch_yahoo_prices(INDICES_AND_COMMODITIES, "indices_commodities_prices.json", batch_size=10)
    
    print("\n" + "=" * 60)
    print("✅ COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()
