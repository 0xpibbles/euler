import dotenv, os
from dune_client.client import DuneClient

dotenv_path = os.path.join(os.path.dirname(__file__), '.', '.env')
dotenv.load_dotenv(".env")
dune = DuneClient.from_env()

table = dune.create_table(
        namespace="0xpibs",
        table_name="euler_liquidations",
        description="liquidations from euler",
        schema= [
            {"name": "block_number", "type": "uint256"},
            {"name": "blockchain", "type": "varchar"},
            {"name": "tx_hash", "type": "varchar"},
            {"name": "collateral_vault_address", "type": "varchar", "nullable": True},
            {"name": "collateral_token_amount", "type": "double", "nullable": True},
            {"name": "collateral_price", "type": "double", "nullable": True},
            {"name": "debt_vault_address", "type": "varchar", "nullable": True},
            {"name": "debt_token_amount", "type": "double", "nullable": True},
            {"name": "debt_price", "type": "double", "nullable": True},
            {"name": "oracle_address", "type": "varchar", "nullable": True},
            {"name": "unit_of_account", "type": "varchar", "nullable": True}
        ],
        is_private=False
)