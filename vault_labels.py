import dotenv
import os
import pandas as pd
import requests
from dune_client.client import DuneClient
from dune_client.query import QueryBase

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(__file__), '.', '.env')
dotenv.load_dotenv(dotenv_path)
dune = DuneClient.from_env()

url = "https://api.dune.com/api/v1/table/pibbles/euler_vault_labels/clear"

headers = {
    "X-DUNE-API-KEY": os.getenv("DUNE_API_KEY")
}

response = requests.request("POST", url, headers=headers)

# URLs for the JSON files
urls = {
    'ethereum': 'https://raw.githubusercontent.com/euler-xyz/euler-labels/refs/heads/master/1/vaults.json',
    'base': 'https://raw.githubusercontent.com/euler-xyz/euler-labels/refs/heads/master/8453/vaults.json',
    'sonic': 'https://raw.githubusercontent.com/euler-xyz/euler-labels/refs/heads/master/146/vaults.json',
    'berachain': 'https://raw.githubusercontent.com/euler-xyz/euler-labels/refs/heads/master/80094/vaults.json',
    'avalanche_c': 'https://raw.githubusercontent.com/euler-xyz/euler-labels/refs/heads/master/43114/vaults.json',
    'bnb': 'https://raw.githubusercontent.com/euler-xyz/euler-labels/refs/heads/master/56/vaults.json',
    'arbitrum': 'https://raw.githubusercontent.com/euler-xyz/euler-labels/refs/heads/master/42161/vaults.json'
    # 'optimism': 'https://raw.githubusercontent.com/euler-xyz/euler-labels/refs/heads/master/10/vaults.json',
    # 'linea': 'https://raw.githubusercontent.com/euler-xyz/euler-labels/refs/heads/master/59144/vaults.json',

}

# Initialize a list to hold the processed data
data = []

# Fetch and process each JSON file
for blockchain, url in urls.items():
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad responses
    json_data = response.json()

    # Process the JSON data
    for key, value in json_data.items():
        vault = key
        name = value.get('name')
        description = value.get('description')
        entity = value.get('entity')

        # Append the processed data to the list
        data.append({
            'blockchain': blockchain,
            'vault': vault,
            'name': name,
            'description': description,
            'entity': entity
        })

# Create a DataFrame from the processed data
df = pd.DataFrame(data)

# Save the DataFrame to a CSV file
csv_file_path = 'euler_vault_labels.csv'
df.to_csv(csv_file_path, index=False)
print(f"Data saved to {csv_file_path}.")

# Upload the CSV to Dune
with open(csv_file_path, "rb") as data_file:
    response = dune.insert_table(
        namespace="pibbles",  # Replace with your namespace
        table_name="euler_vault_labels",
        data=data_file,
        content_type="text/csv"
    )

print("Data uploaded successfully to Dune.")
