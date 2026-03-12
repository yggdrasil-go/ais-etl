import argparse
import logging
import pandas as pd
from src.imf_portwatch_manager import IMFPortWatchManager
from src.trino_manager import TrinoManager

# Logger 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MasterDataETL:
    def __init__(self, dry_run=False):
        self.imf_manager = IMFPortWatchManager()
        self.trino_manager = TrinoManager()
        self.dry_run = dry_run
        if self.dry_run:
            logger.info("!!! DRY-RUN MODE ENABLED - No changes will be made to the database !!!")

    def _cast_value(self, val, target_type):
        """None 값을 처리하며 데이터 타입을 캐스팅합니다."""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        try:
            return target_type(val)
        except:
            return None

    def load_table(self, data_list, table_name):
        """데이터를 테이블에 적재합니다. (단일 MERGE INTO 쿼리로 벌크 UPSERT)"""
        if not data_list:
            logger.warning(f"No data for {table_name}")
            return

        if self.dry_run:
            logger.info(f"[DRY-RUN] Would perform a SINGLE MERGE (UPSERT) for {len(data_list)} records into {table_name}")
            return

        conn = self.trino_manager.get_connection()
        cur = conn.cursor()
        
        try:
            logger.info(f"Preparing a single bulk UPSERT for {len(data_list)} records into {table_name}...")
            
            # 1. VALUES 절의 플레이스홀더 생성 (?, ?, ..., ?)
            # 필드 개수: 22개
            row_placeholders = "(" + ", ".join(["?"] * 22) + ")"
            all_placeholders = ", ".join([row_placeholders] * len(data_list))
            
            # 2. MERGE INTO SQL 작성
            sql = f"""
                MERGE INTO {table_name} t
                USING (VALUES {all_placeholders}) 
                AS s (portid, portname, country, iso3, continent, fullname, lat, lon, 
                      vessel_count_total, vessel_count_container, vessel_count_dry_bulk, 
                      vessel_count_general_cargo, vessel_count_roro, vessel_count_tanker, 
                      industry_top1, industry_top2, industry_top3, 
                      share_country_maritime_import, share_country_maritime_export, 
                      locode, pageid, countrynoaccents)
                ON (t.portid = s.portid)
                WHEN MATCHED THEN
                    UPDATE SET 
                        portname = s.portname, country = s.country, iso3 = s.iso3, 
                        continent = s.continent, fullname = s.fullname, lat = s.lat, lon = s.lon,
                        vessel_count_total = s.vessel_count_total,
                        vessel_count_container = s.vessel_count_container,
                        vessel_count_dry_bulk = s.vessel_count_dry_bulk,
                        vessel_count_general_cargo = s.vessel_count_general_cargo,
                        vessel_count_roro = s.vessel_count_roro,
                        vessel_count_tanker = s.vessel_count_tanker,
                        industry_top1 = s.industry_top1, industry_top2 = s.industry_top2, 
                        industry_top3 = s.industry_top3,
                        share_country_maritime_import = s.share_country_maritime_import,
                        share_country_maritime_export = s.share_country_maritime_export,
                        locode = s.locode, pageid = s.pageid, 
                        countrynoaccents = s.countrynoaccents,
                        updated_at = now()
                WHEN NOT MATCHED THEN
                    INSERT (portid, portname, country, iso3, continent, fullname, lat, lon, 
                            vessel_count_total, vessel_count_container, vessel_count_dry_bulk, 
                            vessel_count_general_cargo, vessel_count_roro, vessel_count_tanker, 
                            industry_top1, industry_top2, industry_top3, 
                            share_country_maritime_import, share_country_maritime_export, 
                            locode, pageid, countrynoaccents, created_at, updated_at)
                    VALUES (s.portid, s.portname, s.country, s.iso3, s.continent, s.fullname, s.lat, s.lon, 
                            s.vessel_count_total, s.vessel_count_container, s.vessel_count_dry_bulk, 
                            s.vessel_count_general_cargo, s.vessel_count_roro, s.vessel_count_tanker, 
                            s.industry_top1, s.industry_top2, s.industry_top3, 
                            s.share_country_maritime_import, s.share_country_maritime_export, 
                            s.locode, s.pageid, s.countrynoaccents, now(), now())
            """

            # 3. 모든 데이터를 하나의 평탄화된 리스트로 변환
            flattened_values = []
            for item in data_list:
                flattened_values.extend([
                    str(item.get('portid')),
                    str(item.get('portname')),
                    self._cast_value(item.get('country'), str),
                    self._cast_value(item.get('ISO3'), str),
                    self._cast_value(item.get('continent'), str),
                    self._cast_value(item.get('fullname'), str),
                    self._cast_value(item.get('lat'), float),
                    self._cast_value(item.get('lon'), float),
                    self._cast_value(item.get('vessel_count_total'), int),
                    self._cast_value(item.get('vessel_count_container'), int),
                    self._cast_value(item.get('vessel_count_dry_bulk'), int),
                    self._cast_value(item.get('vessel_count_general_cargo'), int),
                    self._cast_value(item.get('vessel_count_RoRo'), int),
                    self._cast_value(item.get('vessel_count_tanker'), int),
                    self._cast_value(item.get('industry_top1'), str),
                    self._cast_value(item.get('industry_top2'), str),
                    self._cast_value(item.get('industry_top3'), str),
                    self._cast_value(item.get('share_country_maritime_import'), float),
                    self._cast_value(item.get('share_country_maritime_export'), float),
                    self._cast_value(item.get('LOCODE'), str),
                    self._cast_value(item.get('pageid'), str),
                    self._cast_value(item.get('countrynoaccents'), str)
                ])

            logger.info(f"Executing single-commit MERGE for {table_name}...")
            cur.execute(sql, flattened_values)
            logger.info(f"Successfully loaded {table_name} in a single commit.")

        except Exception as e:
            logger.error(f"Failed to load {table_name}: {e}")
            raise
        finally:
            cur.close()
            conn.close()

    def run(self):
        """전체 마스터 데이터 ETL 실행"""
        logger.info("Starting Master Data One-time Load...")
        # 1. Chokepoints
        logger.info("Processing Chokepoints...")
        self.load_table(self.imf_manager.get_all_chokepoints(), "portwatch.chokepoints_master")
        
        # 2. Ports
        logger.info("Processing Ports...")
        self.load_table(self.imf_manager.get_all_ports(), "portwatch.ports_master")
        
        logger.info("Master Data Load finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IMF PortWatch Master Data One-time Load to Trino")
    parser.add_argument("--dry-run", action="store_true", help="Log actions without modifying the database")
    
    args = parser.parse_args()
    
    etl = MasterDataETL(dry_run=args.dry_run)
    try:
        etl.run()
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        exit(1)
