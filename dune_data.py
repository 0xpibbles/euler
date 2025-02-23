from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase
# import other needed packages
import pandas as pd
import dotenv, os

os.chdir("/Users/pibbles/.cursor-tutor/euler_api")

# load environment variables
dotenv.load_dotenv()

# initialize dune client
dune = DuneClient.from_env()

# Replace with your actual query ID
query_id = '4759879'

# Download the query results in CSV format
try:
    results_csv = dune.download_csv(query=query_id)

    # Save the results to a file
    with open('query_results.csv', 'wb') as f:
        f.write(results_csv.data.getvalue())
    print("Query results downloaded successfully as 'query_results.csv'.")

except Exception as e:
    print(f"An error occurred: {e}")