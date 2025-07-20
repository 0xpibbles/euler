import requests
import csv
from datetime import datetime, timezone
from dune_client.client import DuneClient
from dune_client.query import QueryBase

# Initialize Dune client
dune = DuneClient.from_env()

# 1. Query the latest date from Dune
def get_latest_date_from_dune():
    query = QueryBase(
        query_id=5448746,  # Replace with your actual query ID
        name="Get Latest Date"
    )
    result = dune.run_query(query)
    rows = result.get_rows()
    if rows and rows[0]['latest_date']:
        return rows[0]['latest_date']
    return None

latest_date = get_latest_date_from_dune()
if latest_date:
    latest_date = datetime.fromisoformat(latest_date)

# 2. Fetch API data
url = "https://api.llama.fi/protocol/uniswap"
response = requests.get(url)
response.raise_for_status()
data = response.json()
arbitrum_tvl_list = data["chainTvls"]["Arbitrum"]["tvl"]
chain = "Arbitrum"

# 3. Filter for new data only
new_rows = []
for entry in arbitrum_tvl_list:
    date_iso = datetime.fromtimestamp(entry['date'], timezone.utc)
    if not latest_date or date_iso > latest_date:
        tvl = entry['totalLiquidityUSD']
        new_rows.append([date_iso.isoformat(), chain, tvl])

# 4. If there is new data, append to CSV and upload
if new_rows:
    csv_file_path = 'defillama_uniswap_new.csv'
    with open(csv_file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['date', 'chain', 'tvl_usd'])
        writer.writerows(new_rows)
    print(f"Saved {len(new_rows)} new rows to {csv_file_path}")

    # Upload to Dune
    with open(csv_file_path, "rb") as data:
        response = dune.insert_table(
            namespace="0xpibs",
            table_name="defillama_uniswap_arbitrum",
            data=data,
            content_type="text/csv"
        )
    print("Data uploaded successfully to Dune.")
else:
    print("No new data to upload.")