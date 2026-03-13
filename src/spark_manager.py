import os
import logging
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

# Logger 설정
logger = logging.getLogger(__name__)

class SparkManager:
    def __init__(self):
        # Spark Connect 서버 주소
        self.remote_url = os.getenv("SPARK_REMOTE_URL", "sc://spark-connect-loader-svc.india.svc.cluster.local:15002")
        self._spark = None

    def get_spark(self):
        """Spark Connect 서버에 접속하여 세션 반환 (Lazy Loading)"""
        if self._spark is None:
            logger.info(f"Connecting to Spark Connect Server at {self.remote_url}...")
            try:
                self._spark = SparkSession.builder.remote(self.remote_url).getOrCreate()
                logger.info("Spark Connect Session established.")
            except Exception as e:
                logger.error(f"Failed to connect to Spark Connect Server: {e}")
                raise
        return self._spark

    def fetch_data(self, query: str):
        """SQL 쿼리 결과를 Pandas DataFrame으로 반환 (TrinoManager 호환용)"""
        spark = self.get_spark()
        logger.info(f"Executing Spark SQL: {query}")
        try:
            return spark.sql(query).toPandas()
        except Exception as e:
            logger.error(f"Error fetching data from Spark: {e}")
            raise

    def execute_sql(self, query: str):
        """결과 반환 없이 SQL 실행 (DDL/DML 용)"""
        spark = self.get_spark()
        logger.info(f"Executing Spark SQL (No Return): {query}")
        try:
            spark.sql(query)
        except Exception as e:
            logger.error(f"Error executing Spark SQL: {e}")
            raise

    def set_nessie_commit_metadata(self, author: str, message: str):
        """
        Nessie 커밋에 기록될 메타데이터를 설정합니다.
        (Nessie-Commit-Authors, Nessie-Commit-Message 헤더 활용)
        """
        spark = self.get_spark()
        logger.info(f"Setting Nessie commit metadata: Author={author}, Message={message}")
        
        # 'prod' 카탈로그 기준으로 헤더 설정
        # Spark 3.4+ 및 Nessie Spark Extension이 활성화된 경우 동작합니다.
        spark.conf.set("spark.sql.catalog.prod.header.Nessie-Commit-Authors", author)
        spark.conf.set("spark.sql.catalog.prod.header.Nessie-Commit-Message", message)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    manager = SparkManager()
    spark = manager.get_spark()
    
    # 1. 기본 연결 테스트
    print("\n=== Basic Connection Test ===")
    spark.range(5).show()
    
    # 2. Iceberg 테이블 테스트 (prod.portwatch.chokepoints)
    print("\n=== Iceberg Table Test (prod.portwatch.chokepoints) ===")
    try:
        # 스키마 출력
        df = spark.table("prod.portwatch.chokepoints")
        print("\n[Schema]")
        df.printSchema()

        # 상위 5개 데이터 출력
        print("\n[Top 5 Rows]")
        df.limit(5).show()

        
    except Exception as e:
        logger.error(f"Failed to access Iceberg table: {e}")
