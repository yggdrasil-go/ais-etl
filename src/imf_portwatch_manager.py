import requests
import pandas as pd
import logging
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union
from dotenv import load_dotenv

load_dotenv()

# Logger 설정
logger = logging.getLogger(__name__)

class IMFPortWatchManager:
    def __init__(self):
        # IMF PortWatch API Endpoints (provided by user)
        self.apis = {
            "daily_port": "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Ports_Data/FeatureServer/0/query",
            "daily_chokepoint": "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Chokepoints_Data/FeatureServer/0/query",
            "chokepoints_db": "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/PortWatch_chokepoints_database/FeatureServer/0/query",
            "ports_db": "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/PortWatch_ports_database/FeatureServer/0/query",
        }
        
        # Schema for Master Data (Chokepoints & Ports)
        self.master_fields = [
            "portid", "portname", "country", "ISO3", "continent", "fullname", "lat", "lon",
            "vessel_count_total", "vessel_count_container", "vessel_count_dry_bulk",
            "vessel_count_general_cargo", "vessel_count_RoRo", "vessel_count_tanker",
            "industry_top1", "industry_top2", "industry_top3",
            "share_country_maritime_import", "share_country_maritime_export",
            "LOCODE", "pageid", "countrynoaccents"
        ]

    def _fetch_master_data(self, api_key: str) -> List[Dict]:
        """
        데이터베이스(Chokepoints/Ports)에서 상세 마스터 정보를 가져옵니다.
        """
        url = self.apis[api_key]
        params = {
            "where": "1=1",
            "outFields": ",".join(self.master_fields),
            "f": "json",
            "returnGeometry": "false"
        }

        logger.info(f"Fetching master data from {api_key} database...")
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if "error" in data:
                logger.error(f"ArcGIS API Error ({api_key}): {data['error']}")
                return []

            features = data.get("features", [])
            results = []
            
            for f in features:
                attr = f.get("attributes", {})
                if attr.get("portname"):
                    results.append(attr)
            
            return sorted(results, key=lambda x: x['portname'])

        except Exception as e:
            logger.error(f"Error fetching {api_key} master data: {e}")
            return []

    def get_all_chokepoints(self) -> List[Dict]:
        """모든 Chokepoint의 마스터 정보를 가져옵니다."""
        return self._fetch_master_data("chokepoints_db")

    def get_all_ports(self) -> List[Dict]:
        """모든 Port의 마스터 정보를 가져옵니다."""
        return self._fetch_master_data("ports_db")

    def fetch_stat_data(self, api_key: str, names: Optional[Union[str, List[str]]] = None, days: int = 30) -> pd.DataFrame:
        """
        Daily Port 또는 Chokepoint 통계 데이터를 가져옵니다.
        """
        url = self.apis[api_key]
        start_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        where_clause = f"date >= date '{start_date}'"
        if names:
            if isinstance(names, str): names = [names]
            names_str = "', '".join([name.replace("'", "''") for name in names])
            where_clause += f" AND portname IN ('{names_str}')"

        # 통계 데이터용 OutFields (항목이 많으므로 주요 항목 위주)
        out_fields = "date,portid,portname,n_tanker,n_container,n_dry_bulk,n_general_cargo,n_roro,n_cargo,n_total,capacity"

        params = {
            "where": where_clause,
            "outFields": out_fields,
            "f": "json",
            "orderByFields": "date DESC, portname ASC",
            "resultRecordCount": 2000
        }

        logger.info(f"Fetching statistics from {api_key} since {start_date}...")
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            features = data.get("features", [])
            records = []
            for f in features:
                attr = f.get("attributes", {})
                if attr.get("date"):
                    # 밀리초 타임스탬프 변환 및 Date 컬럼 추가
                    attr["Date"] = datetime.fromtimestamp(attr["date"] / 1000).strftime("%Y-%m-%d")
                records.append(attr)

            df = pd.DataFrame(records)
            if not df.empty and "date" in df.columns:
                df = df.drop(columns=["date"])
            return df

        except Exception as e:
            logger.error(f"Error fetching statistics from {api_key}: {e}")
            raise

    def fetch_chokepoint_data(self, names: Optional[Union[str, List[str]]] = None, days: int = 30) -> pd.DataFrame:
        return self.fetch_stat_data("daily_chokepoint", names, days)

    def fetch_port_data(self, names: Optional[Union[str, List[str]]] = None, days: int = 30) -> pd.DataFrame:
        return self.fetch_stat_data("daily_port", names, days)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    manager = IMFPortWatchManager()
    
    # 1. Chokepoint 마스터 데이터 테스트 (좌표 확인)
    print("\n--- 1. Chokepoint Master Data (Detailed) ---")
    chokepoints = manager.get_all_chokepoints()
    for p in chokepoints: # 상위 3개만 샘플 출력
        print(f"ID: {p['portid']:<12} | Name: {p['portname']:<25} | Loc: ({p['lat']:.4f}, {p['lon']:.4f}) | Country: {p['country']}")
    print(f"Total Chokepoints: {len(chokepoints)}")

    # 2. Port 마스터 데이터 테스트 (샘플)
    print("\n--- 2. Port Master Data (Sample) ---")
    ports = manager.get_all_ports()
    if ports:
        p = ports[0]
        print(f"Sample Port: {p['portname']} ({p['country']}) - Lat: {p['lat']}, Lon: {p['lon']}")
    print(f"Total Ports: {len(ports)}")

    # 3. 실시간 통계 테스트
    try:
        df = manager.fetch_chokepoint_data(names="Strait of Hormuz", days=5)
        if not df.empty:
            print(f"\n--- 3. Recent Stats: Strait of Hormuz ---")
            print(df.to_string(index=False))
    except Exception as e:
        print(f"Test failed: {e}")
