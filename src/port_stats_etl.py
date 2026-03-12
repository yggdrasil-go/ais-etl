import argparse
import logging
import pandas as pd
from datetime import datetime, timedelta
from src.imf_portwatch_manager import IMFPortWatchManager
from src.trino_manager import TrinoManager

# Logger 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PortStatsETL:
    def __init__(self):
        self.imf_manager = IMFPortWatchManager()
        self.trino_manager = TrinoManager()

    def extract(self, target_date: str) -> pd.DataFrame:
        """특정 날짜의 항구 통계 데이터를 정확히 추출합니다."""
        logger.info(f"Extracting Port stats specifically for {target_date}...")
        # start_date와 end_date를 동일하게 설정하여 딱 하루치만 요청
        df = self.imf_manager.fetch_port_data(ids=None, start_date=target_date, end_date=target_date)
        
        if df.empty:
            return pd.DataFrame()

        # API에서 필터링되었지만 안전을 위해 로컬에서도 한 번 더 확인
        df_filtered = df[df['Date'] == target_date].copy()
        
        logger.info(f"Extracted {len(df_filtered)} port rows for {target_date}.")
        return df_filtered

    def load(self, df: pd.DataFrame):
        """추출된 데이터를 portwatch.ports 테이블에 단일 쿼리로 UPSERT(MERGE)합니다."""
        if df.empty:
            logger.warning("No data to load.")
            return

        conn = self.trino_manager.get_connection()
        cur = conn.cursor()
        
        try:
            logger.info(f"Upserting {len(df)} port rows into Trino...")
            
            # 25개 필드 + event_date(CAST) = 총 26개 플레이스홀더
            row_placeholders = "(" + ", ".join(["?"] * 25) + ", CAST(? AS DATE))"
            all_placeholders = ", ".join([row_placeholders] * len(df))
            
            sql = f"""
                MERGE INTO portwatch.ports t
                USING (VALUES {all_placeholders}) 
                AS s (portid, portname, country, iso3, 
                      portcalls_tanker, portcalls_container, portcalls_dry_bulk, portcalls_general_cargo, portcalls_roro, portcalls_cargo, portcalls,
                      import_tanker, import_container, import_dry_bulk, import_general_cargo, import_roro, import_cargo, import,
                      export_tanker, export_container, export_dry_bulk, export_general_cargo, export_roro, export_cargo, export,
                      event_date)
                ON (t.portid = s.portid AND t.event_date = s.event_date)
                WHEN MATCHED AND (
                    t.portcalls != s.portcalls OR t.import != s.import OR t.export != s.export
                ) THEN
                    UPDATE SET 
                        portcalls_tanker = s.portcalls_tanker, portcalls_container = s.portcalls_container,
                        portcalls_dry_bulk = s.portcalls_dry_bulk, portcalls_general_cargo = s.portcalls_general_cargo,
                        portcalls_roro = s.portcalls_roro, portcalls_cargo = s.portcalls_cargo, portcalls = s.portcalls,
                        import_tanker = s.import_tanker, import_container = s.import_container,
                        import_dry_bulk = s.import_dry_bulk, import_general_cargo = s.import_general_cargo,
                        import_roro = s.import_roro, import_cargo = s.import_cargo, import = s.import,
                        export_tanker = s.export_tanker, export_container = s.export_container,
                        export_dry_bulk = s.export_dry_bulk, export_general_cargo = s.export_general_cargo,
                        export_roro = s.export_roro, export_cargo = s.export_cargo, export = s.export,
                        updated_at = now()
                WHEN NOT MATCHED THEN
                    INSERT (portid, portname, country, iso3, 
                            portcalls_tanker, portcalls_container, portcalls_dry_bulk, portcalls_general_cargo, portcalls_roro, portcalls_cargo, portcalls,
                            import_tanker, import_container, import_dry_bulk, import_general_cargo, import_roro, import_cargo, import,
                            export_tanker, export_container, export_dry_bulk, export_general_cargo, export_roro, export_cargo, export,
                            event_date, created_at, updated_at)
                    VALUES (s.portid, s.portname, s.country, s.iso3, 
                            s.portcalls_tanker, s.portcalls_container, s.portcalls_dry_bulk, s.portcalls_general_cargo, s.portcalls_roro, s.portcalls_cargo, s.portcalls,
                            s.import_tanker, s.import_container, s.import_dry_bulk, s.import_general_cargo, s.import_roro, s.import_cargo, s.import,
                            s.export_tanker, s.export_container, s.export_dry_bulk, s.export_general_cargo, s.export_roro, s.export_cargo, s.export,
                            s.event_date, now(), now())
            """
            
            all_values = []
            for _, r in df.iterrows():
                all_values.extend([
                    str(r.get('portid', '')), str(r.get('portname', '')), str(r.get('country', '')), str(r.get('ISO3', '')),
                    int(r.get('portcalls_tanker', 0)), int(r.get('portcalls_container', 0)), int(r.get('portcalls_dry_bulk', 0)),
                    int(r.get('portcalls_general_cargo', 0)), int(r.get('portcalls_roro', 0)), int(r.get('portcalls_cargo', 0)), int(r.get('portcalls', 0)),
                    float(r.get('import_tanker', 0)), float(r.get('import_container', 0)), float(r.get('import_dry_bulk', 0)),
                    float(r.get('import_general_cargo', 0)), float(r.get('import_roro', 0)), float(r.get('import_cargo', 0)), float(r.get('import', 0)),
                    float(r.get('export_tanker', 0)), float(r.get('export_container', 0)), float(r.get('export_dry_bulk', 0)),
                    float(r.get('export_general_cargo', 0)), float(r.get('export_roro', 0)), float(r.get('export_cargo', 0)), float(r.get('export', 0)),
                    str(r.get('Date', ''))
                ])

            cur.execute(sql, all_values)
            logger.info(f"Successfully upserted {len(df)} rows for {df.iloc[0]['Date'] if not df.empty else 'unknown'}.")
            
        except Exception as e:
            logger.error(f"Error during port statistics upsert: {e}")
            raise
        finally:
            cur.close()
            conn.close()

    def run(self, start_date: str, end_date: str):
        """전체 ETL 실행 (하루 단위 루프)"""
        logger.info(f"Starting Port Stats Optimized Backfill: {start_date} to {end_date}")
        
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        current_dt = start_dt
        while current_dt <= end_dt:
            target_date = current_dt.strftime("%Y-%m-%d")
            logger.info(f"--- Processing Day: {target_date} ---")
            
            df = self.extract(target_date)
            if not df.empty:
                self.load(df)
            else:
                logger.warning(f"No data found for {target_date}")
            
            current_dt += timedelta(days=1)
            
        logger.info("Optimized Port Stats Backfill process finished.")

if __name__ == "__main__":
    default_end = (datetime.utcnow() + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    default_start = (datetime.utcnow() - pd.Timedelta(days=9)).strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser(description="IMF PortWatch Port Stats ETL to Trino")
    parser.add_argument("--start_date", default=default_start, help=f"Start date (YYYY-MM-DD), default: {default_start}")
    parser.add_argument("--end_date", default=default_end, help=f"End date (YYYY-MM-DD), default: {default_end}")
    
    args = parser.parse_args()
    
    try:
        etl = PortStatsETL()
        etl.run(args.start_date, args.end_date)
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        exit(1)
