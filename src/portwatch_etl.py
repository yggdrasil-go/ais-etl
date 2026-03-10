import argparse
import logging
import pandas as pd
from datetime import datetime
from src.imf_portwatch_manager import IMFPortWatchManager
from src.trino_manager import TrinoManager

# Logger 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PortWatchETL:
    def __init__(self):
        self.imf_manager = IMFPortWatchManager()
        self.trino_manager = TrinoManager()

    def extract(self, start_date: str, end_date: str) -> pd.DataFrame:
        """IMF PortWatch API로부터 데이터를 추출합니다."""
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        now = datetime.utcnow()
        days_to_fetch = (now - start_dt).days + 1
        
        logger.info(f"Extracting data from IMF PortWatch (days back: {days_to_fetch})...")
        df = self.imf_manager.fetch_chokepoint_data(days=days_to_fetch)
        
        if df.empty:
            logger.warning("No data found during extraction.")
            return pd.DataFrame()

        # 기간 필터링
        df['Date_dt'] = pd.to_datetime(df['Date'])
        mask = (df['Date_dt'] >= start_dt) & (df['Date_dt'] <= datetime.strptime(end_date, "%Y-%m-%d"))
        df_filtered = df.loc[mask].copy()
        df_filtered.drop(columns=['Date_dt'], inplace=True)
        
        logger.info(f"Extracted {len(df_filtered)} relevant rows.")
        return df_filtered

    def load(self, df: pd.DataFrame):
        """추출된 데이터를 Trino/Iceberg 테이블에 적재합니다."""
        if df.empty:
            logger.warning("No data to load.")
            return

        conn = self.trino_manager.get_connection()
        cur = conn.cursor()
        
        # INSERT SQL 작성
        sql = """
            INSERT INTO portwatch.chokepoints (
                portid, portname, n_tanker, n_container, n_dry_bulk, 
                n_general_cargo, n_roro, n_cargo, n_total, capacity, event_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS DATE))
        """
        
        # 데이터 튜플 변환 (Pandas 타입을 Python 기본 타입으로 캐스팅)
        rows = []
        for _, r in df.iterrows():
            rows.append((
                str(r['portid']), 
                str(r['portname']), 
                int(r['n_tanker']), 
                int(r['n_container']),
                int(r['n_dry_bulk']), 
                int(r['n_general_cargo']), 
                int(r['n_roro']),
                int(r['n_cargo']), 
                int(r['n_total']), 
                int(r['capacity']), 
                str(r['Date'])
            ))

        try:
            logger.info(f"Loading {len(rows)} rows into portwatch.chokepoints...")
            cur.executemany(sql, rows)
            logger.info("Load completed successfully.")
        except Exception as e:
            logger.error(f"Error during load: {e}")
            raise
        finally:
            cur.close()
            conn.close()

    def run(self, start_date: str, end_date: str):
        """ETL 프로세스 전체 실행"""
        logger.info(f"Starting ETL: {start_date} to {end_date}")
        df = self.extract(start_date, end_date)
        if not df.empty:
            self.load(df)
        logger.info("ETL process finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IMF PortWatch ETL to Trino/Iceberg")
    parser.add_argument("--start_date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", required=True, help="End date (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    etl = PortWatchETL()
    etl.run(args.start_date, args.end_date)
