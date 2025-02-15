from flask import Flask, jsonify
import requests
from web3 import Web3
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import os
from dune_client.client import DuneClient
from dune_client.types import QueryParameter
from flask_caching import Cache

# Load environment variables from .env file
load_dotenv()

# Initialize Flask app and cache
app = Flask(__name__)
cache = Cache(app, config={'CACHE_TYPE': 'simple'})

# Initialize Dune client with API key
DUNE_API_KEY = os.getenv('DUNE_API_KEY')
dune = DuneClient(DUNE_API_KEY)

# Configure RPC endpoints
RPC_ENDPOINTS = {
    'ethereum': 'https://eth.drpc.org',
    'base': 'https://base-rpc.publicnode.com'
}

# Helper function to make ethereum RPC calls
def make_eth_call(rpc_url, to_address, data, block="latest"):
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
    return response.json()['result']

# Cache the vault index data for 5 minutes
@cache.memoize(timeout=300)
def get_vault_index():
    """Fetch the Euler Finance index data from Dune"""
    query = """
    select distinct
        vault_address,
        blockchain,
        name,
        entity,
        asset_token,
        symbol,
        debt_token
    from dune."0xpibs".result_euler_finance_index
    """
    results = dune.execute_query(query)
    return results.result.rows

# Cache the liquidation events for 5 minutes
@cache.memoize(timeout=300)
def get_liquidation_events():
    """Fetch the liquidation events from Dune"""
    query = """
    select distinct
        evt_block_number,
        chain as blockchain,
        cast(concat('0x', format('%x', evt_block_number)) as varchar) as block_number_hex,
        evt_tx_hash,
        collateral as collateral_vault_address,
        contract_address as debt_vault_address,
        yieldbalance as collateral_token_amount,
        repayassets as debt_token_amount
    from euler_v2_multichain.EVault_evt_Liquidate
    order by evt_block_number desc
    limit 100  -- Limiting to recent events for testing
    """
    results = dune.execute_query(query)
    return results.result.rows

def get_oracle_price(rpc_url, oracle, amount, token, unit_of_account, block):
    """Calculate oracle price for given parameters"""
    # Construct calldata for oracle price check
    calldata = (
        "0xae68676c" +  # Function selector for getPrice()
        Web3.to_hex(int(float(amount)))[2:].zfill(64) +  # Amount
        token[2:].zfill(64) +  # Token address
        unit_of_account[2:].zfill(64)  # Unit of account address
    )
    
    result = make_eth_call(rpc_url, oracle, calldata, block)
    return int(result, 16) / (10 ** 18)  # Convert to decimal

# Main API endpoint
@app.route('/liquidations', methods=['GET'])
def get_liquidations():
    try:
        # Get base data from Dune
        liquidation_events = get_liquidation_events()
        vault_index = get_vault_index()

        # Create lookup dictionary for vault data
        vault_lookup = {v['vault_address']: v for v in vault_index}

        results = []
        for event in liquidation_events:
            try:
                # Get oracle address
                oracle_data = make_eth_call(
                    RPC_ENDPOINTS[event['blockchain']],
                    event['debt_vault_address'],
                    "0x7dc0d1d0"  # Function selector for oracle()
                )
                oracle_address = "0x" + oracle_data[26:66]

                # Get unit of account
                uoa_data = make_eth_call(
                    RPC_ENDPOINTS[event['blockchain']],
                    event['debt_vault_address'],
                    "0x3e833364"  # Function selector for unitOfAccount()
                )
                unit_of_account = "0x" + uoa_data[26:66]

                # Get vault details
                debt_vault = vault_lookup.get(event['debt_vault_address'], {})
                collateral_vault = vault_lookup.get(event['collateral_vault_address'], {})

                # Calculate prices using oracle
                debt_price = get_oracle_price(
                    RPC_ENDPOINTS[event['blockchain']],
                    oracle_address,
                    event['debt_token_amount'],
                    debt_vault.get('asset_token', event['debt_vault_address']),
                    unit_of_account,
                    event['block_number_hex']
                )

                collateral_price = get_oracle_price(
                    RPC_ENDPOINTS[event['blockchain']],
                    oracle_address,
                    event['collateral_token_amount'],
                    event['collateral_vault_address'],
                    unit_of_account,
                    event['block_number_hex']
                )

                results.append({
                    'block_number': event['evt_block_number'],
                    'blockchain': event['blockchain'],
                    'tx_hash': event['evt_tx_hash'],
                    'debt_vault_address': event['debt_vault_address'],
                    'debt_vault_name': debt_vault.get('name', 'Unknown'),
                    'collateral_vault_address': event['collateral_vault_address'],
                    'collateral_vault_name': collateral_vault.get('name', 'Unknown'),
                    'debt_token_amount': event['debt_token_amount'],
                    'collateral_token_amount': event['collateral_token_amount'],
                    'debt_price': debt_price,
                    'collateral_price': collateral_price,
                    'liquidation_bonus': collateral_price - debt_price,
                    'liquidation_bonus_rate': (collateral_price / debt_price) - 1 if debt_price else None,
                    'unit_of_account': unit_of_account
                })

            except Exception as e:
                print(f"Error processing event: {e}")
                continue

        return jsonify({
            'status': 'success',
            'data': results
        })

    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)