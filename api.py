import dotenv
import os
import pandas as pd
import requests
from flask import Flask, jsonify
from dune_client.client import DuneClient
from dune_client.query import QueryBase

# Load environment variables from .env file
dotenv_path = os.path.join(os.path.dirname(__file__), '.', '.env')
dotenv.load_dotenv(dotenv_path)
dune = DuneClient.from_env()

app = Flask(__name__)

def make_eth_call(rpc_url, to_address, data, block="latest"):
    """Make an eth_call to the specified RPC endpoint"""
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{
            "to": to_address,
            "data": data
        }, block],
        "id": 1
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(rpc_url, json=payload, headers=headers)
    return response.json().get('result')

def get_oracle_address(rpc_url, vault_address):
    """Get oracle address from vault contract"""
    try:
        data = "0x7dc0d1d0"
        result = make_eth_call(rpc_url, vault_address, data)
        if result:
            return "0x" + result[26:]  # Extract address from result (skip first 12 bytes)
        return None
    except Exception as e:
        print(f"Error getting oracle address for vault {vault_address}: {str(e)}")
        return None

def get_unit_of_account(rpc_url, vault_address):
    """Get unit of account from vault contract"""
    try:
        data = "0x3e833364"
        result = make_eth_call(rpc_url, vault_address, data)
        if result:
            return "0x" + result[26:]  # Extract address from result (skip first 12 bytes)
        return None
    except Exception as e:
        print(f"Error getting unit of account: {str(e)}")
        return None

def get_oracle_price(rpc_url, oracle_address, amount, token_address, unit_of_account, block):
    """Get price from Euler oracle"""
    try:
        data = "0xae68676c" + \
               format(int(float(amount)), '064x') + \
               token_address.replace('0x', '').zfill(64) + \
               unit_of_account.replace('0x', '').zfill(64)
        
        result = make_eth_call(rpc_url, oracle_address, data, block)
        if result:
            return int(result, 16) / (10 ** 18)
        return None
    except Exception as e:
        print(f"Error getting oracle price: {str(e)}")
        return None

@app.route('/liquidations', methods=['GET'])
def get_liquidations():
    try:
        print("Starting data retrieval...")

        # Create a Query object
        query = QueryBase(
            query_id=4727182,  # Replace with your actual query ID
            name="Euler Liquidations"
        )
        
        # Get base data from Dune
        results = dune.run_query(query)
        
        processed_results = []
        for row in results.get_rows():
            try:
                # Get the RPC URL directly from the row
                rpc_url = row['rpc_url']  # Ensure your Dune query includes this column
                print(f"Processing vault: {row['debt_vault_address']} on blockchain: {row['blockchain']} with RPC: {rpc_url}")  # Debugging output
                if not rpc_url:
                    print(f"Skipping row - no RPC URL for blockchain: {row['blockchain']}")
                    continue

                # Get oracle address from vault contract using the correct RPC
                oracle_address = get_oracle_address(rpc_url, row['debt_vault_address'])
                if not oracle_address:
                    print(f"Skipping row - couldn't get oracle address for vault: {row['debt_vault_address']}")
                    continue

                # Get unit of account from vault contract using the correct RPC
                unit_of_account = get_unit_of_account(rpc_url, row['debt_vault_address'])
                if not unit_of_account:
                    print(f"Skipping row - couldn't get unit of account for vault: {row['debt_vault_address']}")
                    continue

                # Get debt price
                debt_price = get_oracle_price(
                    rpc_url=rpc_url,
                    oracle_address=oracle_address,
                    amount=row['debt_token_amount'],
                    token_address=row['debt_asset_token'],
                    unit_of_account=unit_of_account,
                    block=row['block_number_hex']
                )

                # Get collateral price
                collateral_price = get_oracle_price(
                    rpc_url=rpc_url,
                    oracle_address=oracle_address,
                    amount=row['collateral_token_amount'],
                    token_address=row['collateral_asset_token'],
                    unit_of_account=unit_of_account,
                    block=row['block_number_hex']
                )

                processed_results.append({
                    'block_number': row['evt_block_number'],
                    'blockchain': row['blockchain'],
                    'tx_hash': row['evt_tx_hash'],
                    'collateral_vault_name': row['collateral_vault_name'],
                    'collateral_vault_address': row['collateral_vault_address'],
                    'collateral_token_amount': float(row['collateral_token_amount']),
                    'collateral_price': collateral_price,
                    'debt_vault_name': row['debt_vault_name'],
                    'debt_vault_address': row['debt_vault_address'],
                    'debt_token_amount': float(row['debt_token_amount']),
                    'debt_price': debt_price,
                    'oracle_address': oracle_address,
                    'unit_of_account': unit_of_account    
                })

            except Exception as e:
                print(f"Error processing row: {str(e)}")
                print(f"Row data: {row}")
                continue

        return jsonify(processed_results)

    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
