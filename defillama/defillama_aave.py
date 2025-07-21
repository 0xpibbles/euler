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

# 1. Query the latest date for Aave from Dune
def get_latest_date_from_dune(protocol_name):
    query = QueryBase(
        name="Get Latest Date",
        query_id=5448746,  # <-- Replace with your actual query ID
        params=[
            QueryParameter.text_type(name="protocol", value=protocol_name)
        ]
    )
    result = dune.run_query(query, ping_frequency=15)
    rows = result.get_rows()
    # Parse the latest date from Dune as a date object
    if rows and rows[0]['latest_date']:
        latest_date = datetime.strptime(rows[0]['latest_date'], "%Y-%m-%d %H:%M:%S.%f %Z").date()
    else:
        latest_date = None
    return latest_date

protocol = "Aave"
latest_date = get_latest_date_from_dune(protocol)

# 2. Fetch the TVL from DefiLlama
url = "https://api.llama.fi/protocol/aave"
response = requests.get(url)
response.raise_for_status()
data = response.json()

# 3. Extract historical TVL data and build lookups
arbitrum_tvl_list = data["chainTvls"]["Arbitrum"]["tvl"]
arbitrum_borrowed_list = data["chainTvls"]["Arbitrum-borrowed"]["tvl"]
borrowed_by_date = {entry['date']: entry['totalLiquidityUSD'] for entry in arbitrum_borrowed_list}
chain = "Arbitrum"
protocol = "Aave"

# 4. Process API data to get only the latest entry per day
latest_entries = {}
for entry in arbitrum_tvl_list:
    entry_date = datetime.fromtimestamp(entry['date'], timezone.utc).date()
    latest_entries[entry_date] = entry # Overwrites earlier entries for the same day

# 5. Filter for new data only, using the latest entry for each day
new_rows = []
for entry_date, entry in latest_entries.items():
    if not latest_date or entry_date > latest_date:
        total_liquidity = entry['totalLiquidityUSD']
        borrowed_liquidity = borrowed_by_date.get(entry['date'], None)
        new_rows.append([
            entry_date.isoformat(), protocol, chain, total_liquidity, borrowed_liquidity
        ])

csv_file_path = 'defillama_aave_new.csv'

# 6. Save as CSV (each row: date, protocol, chain, total_liquidity_usd, total_borrowed_liquidity_usd)
if new_rows:
    with open(csv_file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['date', 'protocol', 'chain', 'total_liquidity_usd', 'total_borrowed_liquidity_usd'])
        writer.writerows(new_rows)
    print(f"Saved {len(new_rows)} new rows to {csv_file_path}")

    # 7. Upload to Dune via API
    try:
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
else:
    print("No new data to upload.")
