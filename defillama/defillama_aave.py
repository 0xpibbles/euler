import requests
import csv
from datetime import datetime, timezone
from dune_client.client import DuneClient
import dotenv

# 1. Fetch the TVL from DefiLlama
url = "https://api.llama.fi/protocol/aave"
response = requests.get(url)
response.raise_for_status()
data = response.json()

# 2. Extract historical TVL data for Arbitrum and Arbitrum-borrowed
arbitrum_tvl_list = data["chainTvls"]["Arbitrum"]["tvl"]
arbitrum_borrowed_list = data["chainTvls"]["Arbitrum-borrowed"]["tvl"]
chain = "Arbitrum"

# 3. Build a dict for borrowed TVL by date for fast lookup
borrowed_by_date = {entry['date']: entry['totalLiquidityUSD'] for entry in arbitrum_borrowed_list}

csv_file_path = 'defillama_aave.csv'

# 4. Save as CSV (each row: date, chain, totalLiquidityUSD, totalBorrowedLiquidityUSD)
with open(csv_file_path, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['date', 'chain', 'total_liquidity_usd', 'total_borrowed_liquidity_usd'])
    for entry in arbitrum_tvl_list:
        # Convert UNIX timestamp to ISO8601 date (timezone-aware)
        date_iso = datetime.fromtimestamp(entry['date'], timezone.utc).isoformat()
        total_liquidity = entry['totalLiquidityUSD']
        borrowed_liquidity = borrowed_by_date.get(entry['date'], None)
        writer.writerow([date_iso, chain, total_liquidity, borrowed_liquidity])

print(f"Saved historical Arbitrum TVL and borrowed data to {csv_file_path}")

# 5. Upload to Dune via API
def upload_to_dune(csv_file_path):
    try:
        from dune_client.client import DuneClient
        dune = DuneClient("DUNE_API_KEY")  # <-- Replace with your actual API key
        with open(csv_file_path, "rb") as data:
            response = dune.insert_table(
                namespace="0xpibs",
                table_name="defillama_aave_arbitrum",
                data=data,
                content_type="text/csv"
            )
        print("Data uploaded successfully to Dune.")
        print(response)
    except Exception as e:
        print(f"Error uploading to Dune: {str(e)}")

upload_to_dune("defillama_aave.csv")