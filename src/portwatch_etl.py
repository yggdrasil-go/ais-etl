import argparse
import logging
import pandas as pd
from datetime import datetime
from imf_portwatch_manager import IMFPortWatchManager
from trino_manager import TrinoManager

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
        """추출된 데이터를 Trino/Iceberg 테이블에 UPSERT(MERGE)합니다."""
        if df.empty:
            logger.warning("No data to load.")
            return

        conn = self.trino_manager.get_connection()
        cur = conn.cursor()
        
        # MERGE INTO SQL 작성 (UPSERT 로직)
        # portid, portname, event_date를 복합 키로 사용합니다.
        # 데이터 값이 실제로 변경된 경우에만 UPDATE를 수행합니다.
        sql = """
            MERGE INTO portwatch.chokepoints t
            USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS DATE))) 
            AS s (portid, portname, n_tanker, n_container, n_dry_bulk, 
                  n_general_cargo, n_roro, n_cargo, n_total, capacity, event_date)
            ON (t.portid = s.portid AND t.portname = s.portname AND t.event_date = s.event_date)
            WHEN MATCHED AND (
                t.n_tanker != s.n_tanker OR 
                t.n_container != s.n_container OR 
                t.n_dry_bulk != s.n_dry_bulk OR
                t.n_general_cargo != s.n_general_cargo OR
                t.n_roro != s.n_roro OR
                t.n_cargo != s.n_cargo OR
                t.n_total != s.n_total OR
                t.capacity != s.capacity
            ) THEN
                UPDATE SET 
                    n_tanker = s.n_tanker,
                    n_container = s.n_container,
                    n_dry_bulk = s.n_dry_bulk,
                    n_general_cargo = s.n_general_cargo,
                    n_roro = s.n_roro,
                    n_cargo = s.n_cargo,
                    n_total = s.n_total,
                    capacity = s.capacity,
                    updated_at = now()
            WHEN NOT MATCHED THEN
                INSERT (portid, portname, n_tanker, n_container, n_dry_bulk, 
                        n_general_cargo, n_roro, n_cargo, n_total, capacity, 
                        event_date, created_at, updated_at)
                VALUES (s.portid, s.portname, s.n_tanker, s.n_container, s.n_dry_bulk, 
                        s.n_general_cargo, s.n_roro, s.n_cargo, s.n_total, s.capacity, 
                        s.event_date, now(), now())
        """
        
        # 데이터 튜플 변환
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
            logger.info(f"Upserting {len(rows)} rows into portwatch.chokepoints...")
            for row in rows:
                cur.execute(sql, row)
            logger.info("Upsert completed successfully.")
        except Exception as e:
            logger.error(f"Error during upsert: {e}")
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
    # 기본값 계산: end_date는 현재 UTC 기준 내일, start_date는 그로부터 10일 전
    default_end = (datetime.utcnow() + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    default_start = (datetime.utcnow() - pd.Timedelta(days=9)).strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser(description="IMF PortWatch ETL to Trino/Iceberg")
    parser.add_argument("--start_date", default=default_start, help=f"Start date (YYYY-MM-DD), default: {default_start}")
    parser.add_argument("--end_date", default=default_end, help=f"End date (YYYY-MM-DD), default: {default_end}")
    
    args = parser.parse_args()
    
    try:
        etl = PortWatchETL()
        etl.run(args.start_date, args.end_date)
    except Exception as e:
        logger.error(f"ETL failed with error: {e}")
        raise
