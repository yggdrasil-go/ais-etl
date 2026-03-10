import os
import trino
import pandas as pd
import logging
from dotenv import load_dotenv

load_dotenv()

# Logger 설정
logger = logging.getLogger(__name__)

class TrinoManager:
    def __init__(self):
        self.host = os.getenv("TRINO_URL")
        self.port = int(os.getenv("TRINO_PORT", 8080))
        self.user = os.getenv("TRINO_USER", "gemini-cli")
        self.catalog = os.getenv("TRINO_CATALOG", "nessie")
        self.schema = os.getenv("TRINO_SCHEMA", "gdelt")

    def get_connection(self):
        return trino.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema
        )

    def fetch_data(self, query: str) -> pd.DataFrame:
        logger.info(f"Executing Trino query: {query}")
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute(query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            logger.error(f"Error fetching data from Trino: {e}")
            raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    manager = TrinoManager()
    logger.info(f"TrinoManager initialized for {manager.host}:{manager.port}")
