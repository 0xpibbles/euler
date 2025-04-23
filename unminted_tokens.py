import requests
import csv
import dotenv
import os
import pandas as pd
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from datetime import datetime

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(__file__), '.', '.env')
dotenv.load_dotenv(dotenv_path)
dune = DuneClient.from_env()

# Clear existing table in Dune
url = "https://api.dune.com/api/v1/table/rumpel_protocol/unminted_tokens/clear"
headers = {
    "X-DUNE-API-KEY": os.getenv("DUNE_API_KEY")
}
response = requests.request("POST", url, headers=headers)

# Step 1: Fetch the data from Rumpel API
url = "https://www.app.rumpel.xyz/api/unminted-ptokens"
response = requests.get(url)
response.raise_for_status()
data = response.json()

# Step 2: Flatten the JSON into rows with contract address and readable name
flattened_data = []
for entry in data:
    timestamp = entry.get("timestamp")
    totals = entry.get("totals", {})
    totals_readable = entry.get("totalsReadable", {})
    p_token_addresses = entry.get("pTokenAddresses", {})
    
    for readable_name, value in totals_readable.items():
        # Get the pToken address
        contract_address = p_token_addresses.get(readable_name)
        if contract_address:  # Only add if we have a contract address
            flattened_data.append({
                "timestamp": timestamp,
                "contract_address": contract_address,
                "readable_name": readable_name,
                "value": value
            })

# Create DataFrame
df = pd.DataFrame(flattened_data)

# Save to CSV
csv_file_path = 'unminted_ptokens.csv'
df.to_csv(csv_file_path, index=False)
print(f"✅ CSV saved as: {csv_file_path}")

# Upload the CSV to Dune
with open(csv_file_path, "rb") as data_file:
    response = dune.insert_table(
        namespace="rumpel_protocol",
        table_name="unminted_tokens",
        data=data_file,
        content_type="text/csv"
    )

print("✅ Data uploaded successfully to Dune.")