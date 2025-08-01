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

# 1. Query the latest date for Dolomite from Dune
def get_latest_date_from_dune(protocol_name):
    query = QueryBase(
        name="Get Latest Date",
        query_id=5448746,  # <-- Make sure this is your query ID
        params=[
            QueryParameter.text_type(name="protocol", value=protocol_name)
        ]
    )
    result = dune.run_query(query, ping_frequency=30)
    rows = result.get_rows()
    # Parse the latest date from Dune as a date object
    if rows and rows[0]['latest_date']:
        return datetime.strptime(rows[0]['latest_date'], "%Y-%m-%d %H:%M:%S.%f %Z").date()
    return None

protocol = "Dolomite"
latest_date = get_latest_date_from_dune(protocol)

# 2. Fetch API data
url = "https://api.llama.fi/protocol/dolomite"
response = requests.get(url)
response.raise_for_status()
data = response.json()
arbitrum_tvl_list = data["chainTvls"]["Arbitrum"]["tvl"]
arbitrum_borrowed_list = data["chainTvls"]["Arbitrum-borrowed"]["tvl"]
borrowed_by_date = {entry['date']: entry['totalLiquidityUSD'] for entry in arbitrum_borrowed_list}
chain = "Arbitrum"

# 3. Process API data to get only the latest entry per day
latest_entries = {}
for entry in arbitrum_tvl_list:
    entry_date = datetime.fromtimestamp(entry['date'], timezone.utc).date()
    latest_entries[entry_date] = entry  # Overwrites earlier entries for the same day

# 4. Filter for new data only, using the latest entry for each day
new_rows = []
for entry_date, entry in latest_entries.items():
    if not latest_date or entry_date > latest_date:
        total_liquidity = entry['totalLiquidityUSD']
        borrowed_liquidity = borrowed_by_date.get(entry['date'], None)
        new_rows.append([entry_date.isoformat(), protocol, chain, total_liquidity, borrowed_liquidity])

# 5. If there is new data, append to CSV and upload
csv_file_path = 'defillama_dolomite_new.csv'
if new_rows:
    with open(csv_file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['date', 'protocol', 'chain', 'total_liquidity_usd', 'total_borrowed_liquidity_usd'])
        writer.writerows(new_rows)
    print(f"Saved {len(new_rows)} new rows to {csv_file_path}")

    # Upload to Dune
    try:
        with open(csv_file_path, "rb") as data:
            response = dune.insert_table(
                namespace="entropy_advisors",
                table_name="defillama_dolomite_arbitrum",
                data=data,
                content_type="text/csv"
            )
        print("Data uploaded successfully to Dune.")
        print(response)
    except Exception as e:
        print(f"Error uploading to Dune: {str(e)}")
else:
    print("No new data to upload.")