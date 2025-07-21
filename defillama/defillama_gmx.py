import requests
import csv
from datetime import datetime, timezone
from dune_client.client import DuneClient
from dotenv import load_dotenv
from dune_client.query import QueryBase
from dune_client.types import QueryParameter

# Load environment variables from .env file
load_dotenv()

# Initialize Dune client
dune = DuneClient.from_env()

# 1. Query the latest date for GMX from Dune
def get_latest_date_from_dune(protocol_name):
    query = QueryBase(
        name="Get Latest Date",
        query_id=5448746,  # <-- Replace with your actual query ID
        params=[
            QueryParameter.text_type(name="protocol", value=protocol_name)
        ]
    )
    result = dune.run_query(query)
    rows = result.get_rows()
    if rows and rows[0]['latest_date']:
        # Parse date string like '2025-07-19 00:00:00.000 UTC'
        latest_date = datetime.strptime(rows[0]['latest_date'], "%Y-%m-%d %H:%M:%S.%f %Z")
        latest_date = latest_date.replace(tzinfo=timezone.utc)
        return latest_date
    return None

protocol = "GMX"
latest_date = get_latest_date_from_dune(protocol)

# 2. Fetch the TVL from DefiLlama
url = "https://api.llama.fi/protocol/gmx"
response = requests.get(url)
response.raise_for_status()
data = response.json()

# 2. Extract historical TVL data for Arbitrum and Arbitrum-borrowed
arbitrum_tvl_list = data["chainTvls"]["Arbitrum"]["tvl"]
arbitrum_staking_list = data["chainTvls"]["Arbitrum-staking"]["tvl"]
chain = "Arbitrum"
protocol = "GMX"

# 3. Build a dict for borrowed TVL by date for fast lookup
staking_date = {entry['date']: entry['totalLiquidityUSD'] for entry in arbitrum_staking_list}

# 5. Filter for new data only
new_rows = []
for entry in arbitrum_tvl_list:
    date_iso = datetime.fromtimestamp(entry['date'], timezone.utc)
    if not latest_date or date_iso > latest_date:
        total_liquidity = entry['totalLiquidityUSD']
        borrowed_liquidity = staking_date.get(entry['date'], None)
        new_rows.append([
            date_iso.isoformat(), protocol, chain, total_liquidity, borrowed_liquidity
        ])

csv_file_path = 'defillama_gmx_new.csv'

# 6. If there is new data, append to CSV and upload
if new_rows:
    with open(csv_file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['date', 'protocol', 'chain', 'total_liquidity_usd', 'total_staking_liquidity_usd'])
        writer.writerows(new_rows)
    print(f"Saved {len(new_rows)} new rows to {csv_file_path}")

    # Upload to Dune
    try:
        with open(csv_file_path, "rb") as data:
            response = dune.insert_table(
                namespace="0xpibs",
                table_name="defillama_gmx_arbitrum",
                data=data,
                content_type="text/csv"
            )
        print("Data uploaded successfully to Dune.")
        print(response)
    except Exception as e:
        print(f"Error uploading to Dune: {str(e)}")
else:
    print("No new data to upload.")