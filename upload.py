import dotenv, os
from dune_client.client import DuneClient

dotenv_path = os.path.join(os.path.dirname(__file__), '.', '.env')
dotenv.load_dotenv(".env")
dune = DuneClient.from_env()

table = dune.create_table(
        namespace="0xpibs",
        table_name="euler_vault_labels",
        description="euler vault labels",
        schema= [
            {"name": "blockchain", "type": "varchar"},
            {"name": "vault", "type": "varchar"},
            {"name": "name", "type": "varchar"},
            {"name": "description", "type": "varchar", "nullable": True},
            {"name": "entity", "type": "varchar", "nullable": True}
        ],
        is_private=False
)