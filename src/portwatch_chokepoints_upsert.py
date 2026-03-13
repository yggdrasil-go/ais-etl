import argparse
import logging
import pandas as pd
from datetime import datetime
from imf_portwatch_manager import IMFPortWatchManager
from spark_manager import SparkManager

# Logger 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PortWatchETL:
    def __init__(self):
        self.imf_manager = IMFPortWatchManager()
        self.spark_manager = SparkManager()

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
        """추출된 데이터를 Spark Connect를 통해 UPSERT(MERGE)합니다."""
        if df.empty:
            logger.warning("No data to load.")
            return

        from pyspark.sql import functions as F
        spark = self.spark_manager.get_spark()
        
        try:
            logger.info(f"Uploading {len(df)} rows to Spark and merging into Iceberg (chokepoints)...")
            
            # 1. Pandas DataFrame -> Spark DataFrame 변환 및 전처리
            sdf = spark.createDataFrame(df)
            
            prepared_df = sdf.select(
                F.col("portid").cast("string"),
                F.col("portname").cast("string"),
                F.col("n_tanker").cast("int"),
                F.col("n_container").cast("int"),
                F.col("n_dry_bulk").cast("int"),
                F.col("n_general_cargo").cast("int"),
                F.col("n_roro").cast("int"),
                F.col("n_cargo").cast("int"),
                F.col("n_total").cast("int"),
                F.col("capacity").cast("long"),
                F.col("Date").alias("event_date").cast("date")
            )
            
            prepared_df.createOrReplaceTempView("chokepoint_updates")
            
            # 2. Nessie 커밋 메타데이터 설정 (작성자, 메시지)
            row_count = len(df)
            start_date = df['Date'].min()
            end_date = df['Date'].max()
            
            self.spark_manager.set_nessie_commit_metadata(
                author="AIS ETL Pipeline",
                message=f"Upsert {row_count} rows into portwatch.chokepoints (Range: {start_date} to {end_date})"
            )
            
            # 3. MERGE INTO SQL (prod 카탈로그)
            sql = """
                MERGE INTO prod.portwatch.chokepoints t
                USING chokepoint_updates s
                ON (t.portid = s.portid AND t.event_date = s.event_date)
                WHEN MATCHED THEN
                    UPDATE SET 
                        n_tanker = s.n_tanker,
                        n_container = s.n_container,
                        n_dry_bulk = s.n_dry_bulk,
                        n_general_cargo = s.n_general_cargo,
                        n_roro = s.n_roro,
                        n_cargo = s.n_cargo,
                        n_total = s.n_total,
                        capacity = s.capacity,
                        updated_at = current_timestamp()
                WHEN NOT MATCHED THEN
                    INSERT (portid, portname, n_tanker, n_container, n_dry_bulk, 
                            n_general_cargo, n_roro, n_cargo, n_total, capacity, 
                            event_date, created_at, updated_at)
                    VALUES (s.portid, s.portname, s.n_tanker, s.n_container, s.n_dry_bulk, 
                            s.n_general_cargo, s.n_roro, s.n_cargo, s.n_total, s.capacity, 
                            s.event_date, current_timestamp(), current_timestamp())
            """
            
            spark.sql(sql)
            logger.info("Successfully upserted chokepoint data via Spark.")
            
        except Exception as e:
            logger.error(f"Error during chokepoint upsert via Spark: {e}")
            raise

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
