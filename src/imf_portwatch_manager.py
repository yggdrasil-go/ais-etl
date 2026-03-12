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
        # IMF PortWatch API Endpoints
        self.apis = {
            "daily_port": "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Ports_Data/FeatureServer/0/query",
            "daily_chokepoint": "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Chokepoints_Data/FeatureServer/0/query",
            "chokepoints_db": "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/PortWatch_chokepoints_database/FeatureServer/0/query",
            "ports_db": "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/PortWatch_ports_database/FeatureServer/0/query",
        }
        
        # Schema for Master Data
        self.master_fields = [
            "portid", "portname", "country", "ISO3", "continent", "fullname", "lat", "lon",
            "vessel_count_total", "vessel_count_container", "vessel_count_dry_bulk",
            "vessel_count_general_cargo", "vessel_count_RoRo", "vessel_count_tanker",
            "industry_top1", "industry_top2", "industry_top3",
            "share_country_maritime_import", "share_country_maritime_export",
            "LOCODE", "pageid", "countrynoaccents"
        ]

    def _fetch_master_data(self, api_key: str) -> List[Dict]:
        url = self.apis[api_key]
        results = []
        offset = 0
        batch_size = 1000
        while True:
            params = {
                "where": "1=1",
                "outFields": ",".join(self.master_fields),
                "f": "json",
                "returnGeometry": "false",
                "resultOffset": offset,
                "resultRecordCount": batch_size,
                "orderByFields": "portid ASC"
            }
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                if "error" in data:
                    logger.error(f"ArcGIS API Error ({api_key}): {data['error']}")
                    break
                features = data.get("features", [])
                if not features:
                    break
                for f in features:
                    attr = f.get("attributes", {})
                    if attr.get("portname"):
                        results.append(attr)
                if not data.get("exceededTransferLimit"):
                    break
                offset += len(features)
            except Exception as e:
                logger.error(f"Error fetching {api_key} master data at offset {offset}: {e}")
                break
        return sorted(results, key=lambda x: x['portname'])

    def get_all_chokepoints(self) -> List[Dict]:
        return self._fetch_master_data("chokepoints_db")

    def get_all_ports(self) -> List[Dict]:
        return self._fetch_master_data("ports_db")

    def fetch_stat_data(self, api_key: str, ids: Optional[Union[str, List[str]]] = None, 
                        days: Optional[int] = None, start_date: Optional[str] = None, 
                        end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Daily Port 또는 Chokepoint 통계 데이터를 페이지네이션을 사용하여 모두 가져옵니다.
        특정 날짜 범위 또는 최근 N일(days)을 지정할 수 있습니다.
        """
        url = self.apis[api_key]
        
        # 날짜 조건 생성
        if start_date and end_date:
            where_clause = f"date >= date '{start_date}' AND date <= date '{end_date}'"
        elif start_date:
            where_clause = f"date >= date '{start_date}'"
        elif days is not None:
            calc_start = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
            where_clause = f"date >= date '{calc_start}'"
        else:
            # 기본값: 최근 30일
            calc_start = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")
            where_clause = f"date >= date '{calc_start}'"
        
        if api_key == "daily_chokepoint":
            out_fields = "date,portid,portname,n_tanker,n_container,n_dry_bulk,n_general_cargo,n_roro,n_cargo,n_total,capacity"
            filter_field = "portname"
        else: # daily_port
            out_fields = ("date,portid,portname,country,ISO3,"
                          "portcalls_tanker,portcalls_container,portcalls_dry_bulk,portcalls_general_cargo,portcalls_roro,portcalls_cargo,portcalls,"
                          "import_tanker,import_container,import_dry_bulk,import_general_cargo,import_roro,import_cargo,import,"
                          "export_tanker,export_container,export_dry_bulk,export_general_cargo,export_roro,export_cargo,export")
            filter_field = "portid"

        if ids:
            if isinstance(ids, str): ids = [ids]
            ids_str = "', '".join([str(i).replace("'", "''") for i in ids])
            where_clause += f" AND {filter_field} IN ('{ids_str}')"

        all_records = []
        offset = 0
        batch_size = 1000
        logger.info(f"Fetching statistics from {api_key} with WHERE: {where_clause}")
        
        while True:
            params = {
                "where": where_clause,
                "outFields": out_fields,
                "f": "json",
                "resultOffset": offset,
                "resultRecordCount": batch_size,
                "orderByFields": f"{filter_field} ASC, date DESC"
            }
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if "error" in data:
                    logger.error(f"ArcGIS API Error ({api_key}): {data['error']}")
                    break

                features = data.get("features", [])
                if not features:
                    break

                for f in features:
                    attr = f.get("attributes", {})
                    raw_date = attr.get("date")
                    if raw_date:
                        if isinstance(raw_date, (int, float)):
                            attr["Date"] = datetime.fromtimestamp(raw_date / 1000).strftime("%Y-%m-%d")
                        else:
                            attr["Date"] = str(raw_date)
                    all_records.append(attr)

                logger.info(f"Progress: offset {offset}, total fetched: {len(all_records)}")

                if not data.get("exceededTransferLimit"):
                    break
                offset += len(features)
            except Exception as e:
                logger.error(f"Error fetching statistics from {api_key} at offset {offset}: {e}")
                raise

        df = pd.DataFrame(all_records)
        if not df.empty and "date" in df.columns:
            df = df.drop(columns=["date"])
        return df

    def fetch_chokepoint_data(self, names: Optional[Union[str, List[str]]] = None, days: int = 30) -> pd.DataFrame:
        return self.fetch_stat_data("daily_chokepoint", ids=names, days=days)

    def fetch_port_data(self, ids: Optional[Union[str, List[str]]] = None, days: Optional[int] = None, 
                        start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        return self.fetch_stat_data("daily_port", ids=ids, days=days, start_date=start_date, end_date=end_date)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    manager = IMFPortWatchManager()
    
    # 특정 날짜 하루만 조회 테스트
    try:
        df = manager.fetch_port_data(start_date="2026-03-01", end_date="2026-03-01")
        if not df.empty:
            print(f"\n--- One Day Test (2026-03-01): {len(df)} records ---")
            print(df.head(2).to_string(index=False))
    except Exception as e:
        print(f"One-day test failed: {e}")
