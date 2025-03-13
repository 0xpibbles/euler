import pandas as pd
import requests
from io import StringIO
from dune_client.client import DuneClient
from dune_client.types import QueryParameter
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Initialize Dune client
dune = DuneClient.from_env()

# Fetch API Data
API_URL = "https://app.rumpel.xyz/api/strategies"

try:
    response = requests.get(API_URL)
    response.raise_for_status()
    data = response.text
    print("✅ API data fetched successfully.")
except requests.RequestException as e:
    print(f"⚠️ Error fetching API data: {e}")
    exit(1)

# Parse API Data
data_io = StringIO(data)
# Skip the first row (header) and use our own column names
df = pd.read_csv(data_io, skiprows=1, header=None, names=["strategy_name", "symbol_address", "point_earned", "is_token"])

# Split multi-asset values into separate rows
df["symbol_address"] = df["symbol_address"].str.split("|")
df = df.explode("symbol_address", ignore_index=True)

# Split 'symbol_address' into 'symbol' and 'address'
df[["symbol", "address"]] = df["symbol_address"].str.split(":", n=1, expand=True)
df = df.drop(columns=["symbol_address"])

# Convert is_token to proper boolean
df['is_token'] = df['is_token'].fillna(False)  # Fill NaN values with False
df['is_token'] = df['is_token'].astype(str).map({'True': True, 'False': False})

# Ensure correct column order and remove duplicates
df = df[["strategy_name", "symbol", "address", "point_earned", "is_token"]].drop_duplicates()

# Print first few rows to verify
print("\nFirst few rows of processed data:")
print(df.head())
print("\nSample of is_token values:")
print(df['is_token'].value_counts())

# Save to local CSV file
df.to_csv("vault_labels.csv", index=False)
print("\n✅ Data saved to vault_labels.csv")

# Upload to Dune using CSV format
try:
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    response = dune.insert_table(
        namespace="rumpel_protocol",
        table_name="labels",
        data=csv_buffer.getvalue(),
        content_type="text/csv"
    )
    print("\n✅ Data uploaded successfully to Dune.")
except Exception as e:
    print(f"\n⚠️ Error uploading data to Dune: {e}")