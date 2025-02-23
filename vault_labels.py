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

# URLs for the JSON files
urls = {
    'ethereum': 'https://raw.githubusercontent.com/euler-xyz/euler-labels/refs/heads/master/1/vaults.json',
    'base': 'https://raw.githubusercontent.com/euler-xyz/euler-labels/refs/heads/master/8453/vaults.json',
    'sonic': 'https://raw.githubusercontent.com/euler-xyz/euler-labels/refs/heads/master/146/vaults.json'
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
        namespace="0xpibs",  # Replace with your namespace
        table_name="euler_vault_labels",
        data=data_file,
        content_type="text/csv"
    )

print("Data uploaded successfully to Dune.")