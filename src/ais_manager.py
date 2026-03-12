import os
import json
import asyncio
import websockets
import logging
from datetime import datetime
from typing import List, Optional
from dotenv import load_dotenv

load_dotenv()

# Logger 설정
logger = logging.getLogger(__name__)

class AISManager:
    def __init__(self, bbox_list: Optional[List[List[List[float]]]] = None):
        """
        AISManager 초기화
        :param bbox_list: [[[lat1, lon1], [lat2, lon2]], ...] 형식의 Bounding Box 리스트
        """
        self.api_key = os.getenv("AISSTREAM_API_KEY")
        if not self.api_key:
            raise ValueError("AISSTREAM_API_KEY not found in environment variables.")
        
        self.url = "wss://stream.aisstream.io/v0/stream"
        
        # 주입된 bbox_list가 없으면 bbox.json 로드 시도
        if bbox_list:
            self.bboxes = bbox_list
        else:
            self.bboxes = self._load_bboxes_from_json()

    def _load_bboxes_from_json(self) -> List[List[List[float]]]:
        """bbox.json 파일에서 Bounding Box 리스트를 로드합니다."""
        json_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "bbox.json")
        try:
            if os.path.exists(json_path):
                with open(json_path, "r") as f:
                    data = json.load(f)
                    # [[SW], [NE]] 리스트만 추출
                    return [item["bbox"] for item in data]
            else:
                logger.warning("bbox.json not found. Using default Hormuz BBOX.")
                return [[[25.7969, 56.3598], [26.7969, 57.3598]]]
        except Exception as e:
            logger.error(f"Error loading bbox.json: {e}")
            return [[[25.7969, 56.3598], [26.7969, 57.3598]]]

    async def stream_traffic(self, duration_sec: int = 60):
        """
        설정된 Bounding Box 구역의 실시간 AIS 데이터를 지정된 시간(초) 동안 수집합니다.
        """
        logger.info(f"Connecting to AIS Stream for {len(self.bboxes)} zones (Duration: {duration_sec}s)...")
        
        try:
            async with websockets.connect(self.url) as websocket:
                subscribe_message = {
                    "APIKey": self.api_key,
                    "BoundingBoxes": self.bboxes,
                }
                
                # 반드시 await를 사용하여 메시지 전송
                await websocket.send(json.dumps(subscribe_message))
                logger.info("Subscription message sent. Waiting for AIS data...")

                start_time = datetime.now()
                msg_count = 0
                unique_mmsis = set()

                async for message in websocket:
                    # 경과 시간 체크
                    if (datetime.now() - start_time).total_seconds() > duration_sec:
                        logger.info(f"Stream duration ({duration_sec}s) reached. Closing connection.")
                        break

                    data = json.loads(message)
                    msg_type = data.get("MessageType")
                    metadata = data.get("MetaData", {})
                    mmsi = metadata.get("MMSI")
                    ship_name = metadata.get("ShipName", "").strip()
                    
                    if mmsi:
                        unique_mmsis.add(mmsi)
                        msg_count += 1
                        
                        if msg_type in ["PositionReport", "StandardClassBPositionReport"]:
                            lat = metadata.get("latitude")
                            lon = metadata.get("longitude")
                            logger.info(f"[{msg_type}] {ship_name} (MMSI: {mmsi}) at ({lat}, {lon})")
                        elif msg_type == "ShipStaticData":
                            msg_body = data.get("Message", {}).get("ShipStaticData", {})
                            dest = msg_body.get("Destination", "Unknown")
                            logger.info(f"[Static] {ship_name} (MMSI: {mmsi}) heading to {dest}")

                logger.info(f"Streaming finished. Total messages: {msg_count}, Unique Ships: {len(unique_mmsis)}")
                return {
                    "total_messages": msg_count,
                    "unique_ships": len(unique_mmsis)
                }

        except Exception as e:
            logger.error(f"Error during AIS streaming: {e}")
            raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # 1. 특정 구역만 주입하여 초기화 테스트 (호르무즈)
    # custom_bbox = [[[25.7969, 56.3598], [26.7969, 57.3598]]]
    # manager = AISManager(bbox_list=custom_bbox)
    
    # 2. 기본값(bbox.json)으로 초기화 테스트 (전체 28개 구역)
    manager = AISManager()
    
    # 30초 동안 테스트 수집
    asyncio.run(manager.stream_traffic(duration_sec=30))
