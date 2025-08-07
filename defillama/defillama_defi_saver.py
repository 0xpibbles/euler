import requests
import csv
import json
from datetime import datetime, timezone
from dune_client.client import DuneClient
from dotenv import load_dotenv
from dune_client.query import QueryBase
from dune_client.types import QueryParameter

# Load environment variables from .env file
load_dotenv()

# Initialize Dune client
dune = DuneClient.from_env()

# 1. Query the latest date for Euler from Dune
def get_latest_date_from_dune(protocol_name):
    query = QueryBase(
        name="Get Latest Date",
        query_id=5448746,  # <-- Replace with your actual query ID
        params=[
            QueryParameter.text_type(name="protocol", value=protocol_name)
        ]
    )
    result = dune.run_query(query, ping_frequency=30)
    rows = result.get_rows()
    # Parse the latest date from Dune as a date object
    if rows and rows[0]['latest_date']:
        latest_date = datetime.strptime(rows[0]['latest_date'], "%Y-%m-%d %H:%M:%S.%f %Z").date()
    else:
        latest_date = None
    return latest_date

protocol = "Defi Saver"
latest_date = get_latest_date_from_dune(protocol)

# 2. Fetch the TVL from DefiLlama
url = "https://api.llama.fi/protocol/defi-saver"
response = requests.get(url)
response.raise_for_status()
data = response.json()

# 3. Extract historical TVL data and build lookups
chain_tvls = data["chainTvls"]

suffixes = ["-borrowed", "-staking", "-pool2"]
invalid_chain_names = {"borrowed", "staking", "pool2"}

chains = set()
for key in chain_tvls.keys():
    # Strip suffixes
    for suffix in suffixes:
        if key.endswith(suffix):
            key = key.replace(suffix, "")
            break
    # Filter out invalid chain names
    if key.lower() not in invalid_chain_names:
        chains.add(key)

protocol = data["name"]  # Use actual name from API

output_columns = [
    'date',
    'protocol',
    'chain',
    'total_liquidity_usd',
    'total_borrowed_liquidity_usd',
    'pool2_liquidity_usd',
    'staking_liquidity_usd'
]

new_rows = []

for chain in chains:
    tvl_list = chain_tvls.get(chain, {}).get("tvl", [])
    borrowed_list = chain_tvls.get(f"{chain}-borrowed", {}).get("tvl", [])
    pool2_list = chain_tvls.get(f"{chain}-pool2", {}).get("tvl", [])
    staking_list = chain_tvls.get(f"{chain}-staking", {}).get("tvl", [])

    # Create lookup maps
    has_separate_borrowed = bool(borrowed_list)
    borrowed_by_date = {entry["date"]: entry["totalLiquidityUSD"] for entry in borrowed_list}
    pool2_by_date = {entry["date"]: entry["totalLiquidityUSD"] for entry in pool2_list}
    staking_by_date = {entry["date"]: entry["totalLiquidityUSD"] for entry in staking_list}

    latest_entries = {}
    for entry in tvl_list:
        if isinstance(entry, str):
            entry = json.loads(entry)
        entry_date = datetime.fromtimestamp(entry["date"], timezone.utc).date()
        latest_entries[entry_date] = entry

    for entry_date, entry in latest_entries.items():
        if not latest_date or entry_date > latest_date:
            timestamp = entry["date"]
            total_liquidity = entry["totalLiquidityUSD"]

            # Borrowed liquidity: use separate table if exists, otherwise fallback to in-entry
            if has_separate_borrowed:
                borrowed = borrowed_by_date.get(timestamp, 0)
            else:
                borrowed = entry.get("borrowedLiquidityUSD", 0)

            pool2 = pool2_by_date.get(timestamp, 0)
            staking = staking_by_date.get(timestamp, 0)

            new_rows.append([
                entry_date.isoformat(),
                protocol,
                chain.lower(),
                total_liquidity,
                borrowed,
                pool2,
                staking
            ])

csv_file_path = 'defillama_defi_saver_new.csv'

# 6. Save as CSV (each row: date, protocol, chain, total_liquidity_usd, total_borrowed_liquidity_usd)
if new_rows:
    with open(csv_file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['date', 'protocol', 'chain', 'total_liquidity_usd', 'total_borrowed_liquidity_usd', 'pool2_liquidity_usd', 'staking_liquidity_usd'])
        writer.writerows(new_rows)
    print(f"Saved {len(new_rows)} new rows to {csv_file_path}")

    # 7. Upload to Dune via API
    try:
        with open(csv_file_path, "rb") as data:
            response = dune.insert_table(
                namespace="entropy_advisors",
                table_name="defillama_protocol_tvl",
                data=data,
                content_type="text/csv"
            )
        print("Data uploaded successfully to Dune.")
        print(response)
    except Exception as e:
        print(f"Error uploading to Dune: {str(e)}")
else:
    print("No new data to upload.")
