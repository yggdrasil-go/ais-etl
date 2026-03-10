import logging
from src.trino_manager import TrinoManager

# Logger 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_portwatch_tables():
    manager = TrinoManager()
    conn = manager.get_connection()
    cur = conn.cursor()

    try:
        # 1. 기존 테이블 드랍
        logger.info("Dropping existing tables...")
        cur.execute("DROP TABLE IF EXISTS portwatch.chokepoints")
        cur.execute("DROP TABLE IF EXISTS portwatch.chokepoint_stats")
        
        # 2. 신규 통합 테이블 생성 (portwatch.chokepoints)
        logger.info("Creating integrated table 'portwatch.chokepoints' for statistics...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS portwatch.chokepoints (
                portid VARCHAR,
                portname VARCHAR,
                n_tanker INTEGER,
                n_container INTEGER,
                n_dry_bulk INTEGER,
                n_general_cargo INTEGER,
                n_roro INTEGER,
                n_cargo INTEGER,
                n_total INTEGER,
                capacity BIGINT,
                event_date DATE
            )
            WITH (
                format = 'PARQUET',
                partitioning = ARRAY['month(event_date)']
            )
        """)
        
        logger.info("Successfully recreated portwatch.chokepoints table.")

    except Exception as e:
        logger.error(f"Error during setup: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    setup_portwatch_tables()
