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
        query_id=5448746,  # <-- Make sure this is your query ID
        params=[
            QueryParameter.text_type(name="protocol", value=protocol_name)
        ]
    )
    result = dune.run_query(query)
    rows = result.get_rows()
    # Parse the latest date from Dune as a date object
    if rows and rows[0]['latest_date']:
        # We get a string like '2025-07-19 00:00:00.000 UTC', parse it and get only the .date()
        return datetime.strptime(rows[0]['latest_date'], "%Y-%m-%d %H:%M:%S.%f %Z").date()
    return None

protocol = "Aave"
latest_date = get_latest_date_from_dune(protocol)

# 2. Fetch the TVL from DefiLlama
url = "https://api.llama.fi/protocol/aave"
response = requests.get(url)
response.raise_for_status()
data = response.json()

# 3. Extract historical TVL data for Arbitrum and Arbitrum-borrowed
arbitrum_tvl_list = data["chainTvls"]["Arbitrum"]["tvl"]
arbitrum_borrowed_list = data["chainTvls"]["Arbitrum-borrowed"]["tvl"]
chain = "Arbitrum"
protocol = "Aave"

# 4. Build a dict for borrowed TVL by date for fast lookup
borrowed_by_date = {entry['date']: entry['totalLiquidityUSD'] for entry in arbitrum_borrowed_list}

# 5. Filter for new data only based on the DAY
new_rows = []
for entry in arbitrum_tvl_list:
    # Get just the date part from the API timestamp
    entry_date = datetime.fromtimestamp(entry['date'], timezone.utc).date()
    # Compare dates (not datetimes)
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
