import argparse
import logging
import pandas as pd
from datetime import datetime, timedelta
from imf_portwatch_manager import IMFPortWatchManager
from spark_manager import SparkManager

# Logger 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PortStatsETL:
    def __init__(self):
        self.imf_manager = IMFPortWatchManager()
        self.spark_manager = SparkManager()

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
        """추출된 데이터를 portwatch.ports 테이블에 Spark Connect를 통해 UPSERT(MERGE)합니다."""
        if df.empty:
            logger.warning("No data to load.")
            return

        from pyspark.sql import functions as F
        spark = self.spark_manager.get_spark()
        
        try:
            logger.info(f"Uploading {len(df)} rows to Spark and merging into Iceberg...")
            
            # 1. Pandas DataFrame -> Spark DataFrame 변환
            # (pd.concat으로 인한 Arrow ChunkedArray 에러 방지를 위해 dict 리스트로 변환하여 업로드)
            data_list = df.to_dict('records')
            sdf = spark.createDataFrame(data_list)
            
            # DataFrame API를 사용하여 데이터 타입 및 컬럼명 정리
            # API에서 내려주는 원래 컬럼명을 확인하여 매핑 (ISO3, Date 등)
            prepared_df = sdf.select(
                F.col("portid").cast("string"),
                F.col("portname").cast("string"),
                F.col("country").cast("string"),
                F.col("ISO3").alias("iso3").cast("string"),
                F.col("portcalls_tanker").cast("int"),
                F.col("portcalls_container").cast("int"),
                F.col("portcalls_dry_bulk").cast("int"),
                F.col("portcalls_general_cargo").cast("int"),
                F.col("portcalls_roro").cast("int"),
                F.col("portcalls_cargo").cast("int"),
                F.col("portcalls").cast("int"),
                F.col("import_tanker").cast("double"),
                F.col("import_container").cast("double"),
                F.col("import_dry_bulk").cast("double"),
                F.col("import_general_cargo").cast("double"),
                F.col("import_roro").cast("double"),
                F.col("import_cargo").cast("double"),
                F.col("import").cast("double"),
                F.col("export_tanker").cast("double"),
                F.col("export_container").cast("double"),
                F.col("export_dry_bulk").cast("double"),
                F.col("export_general_cargo").cast("double"),
                F.col("export_roro").cast("double"),
                F.col("export_cargo").cast("double"),
                F.col("export").cast("double"),
                F.col("Date").alias("event_date").cast("date")
            )
            
            # 2. 전처리된 데이터를 임시 뷰로 등록
            prepared_df.createOrReplaceTempView("updates")
            
            # 3. Nessie 커밋 메타데이터 설정 (작성자, 메시지)
            row_count = len(df)
            start_date = df['Date'].min()
            end_date = df['Date'].max()
            
            self.spark_manager.set_nessie_commit_metadata(
                author="AIS ETL Pipeline",
                message=f"Upsert {row_count} rows into portwatch.ports (Range: {start_date} to {end_date})"
            )
            
            # 4. 훨씬 간결해진 MERGE INTO 쿼리 실행
            sql = """
                MERGE INTO prod.portwatch.ports t
                USING updates s
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
                        updated_at = current_timestamp()
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
                            s.event_date, current_timestamp(), current_timestamp())
            """
            
            spark.sql(sql)
            logger.info(f"Successfully upserted data via Spark DataFrame API & Merge.")
            
        except Exception as e:
            logger.error(f"Error during port statistics upsert via Spark: {e}")
            raise

    def run(self, start_date: str, end_date: str):
        """전체 ETL 실행 (데이터 수집 후 단일 커밋 적재)"""
        logger.info(f"Starting Port Stats Optimized Backfill: {start_date} to {end_date}")
        
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        all_data = []
        current_dt = start_dt
        while current_dt <= end_dt:
            target_date = current_dt.strftime("%Y-%m-%d")
            logger.info(f"--- Extracting Day: {target_date} ---")
            
            df = self.extract(target_date)
            if not df.empty:
                all_data.append(df)
            else:
                logger.warning(f"No data found for {target_date}")
            
            current_dt += timedelta(days=1)
            
        if all_data:
            # 모든 날짜의 데이터를 하나로 합침
            final_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"Total collected rows: {len(final_df)}. Starting single-commit load...")
            self.load(final_df)
        else:
            logger.warning("No data collected for the entire range.")

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
