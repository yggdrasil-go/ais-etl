import os
import json
import asyncio
import websockets
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Logger 설정
logger = logging.getLogger(__name__)

class AISManager:
    def __init__(self):
        self.api_key = os.getenv("AISSTREAM_API_KEY")
        if not self.api_key:
            raise ValueError("AISSTREAM_API_KEY not found in environment variables.")
        
        self.url = "wss://stream.aisstream.io/v0/stream"
        # 호르무즈 해협 Bounding Box 설정 (위도, 경도)
        # [남서쪽 위경도], [북동쪽 위경도]
        self.hormuz_bbox = [
            # [24.0, 53.0], 
            # [28.0, 58.0]
            # [-180, -90],
            # [180, 90],
            [-11, 178], [30, 74]
        ]

    async def stream_hormuz_traffic(self, duration_sec: int = 60):
        """
        호르무즈 해협의 실시간 AIS 데이터를 지정된 시간(초) 동안 수집합니다.
        """
        logger.info(f"Connecting to AIS Stream for Strait of Hormuz (Duration: {duration_sec}s)...")
        
        try:
            async with websockets.connect(self.url) as websocket:
                subscribe_message = {
                    "APIKey": self.api_key,
                    "BoundingBoxes": [self.hormuz_bbox],
                    # "FilterMessageTypes": ["PositionReport", "ShipStaticData"]
                }
                
                websocket.send(json.dumps(subscribe_message))
                logger.info("Subscription message sent. Waiting for AIS data...")

                start_time = datetime.now()
                ship_count = 0
                unique_mmsis = set()

                async for message in websocket:
                    # 경과 시간 체크
                    if (datetime.now() - start_time).total_seconds() > duration_sec:
                        logger.info(f"Stream duration ({duration_sec}s) reached. Closing connection.")
                        break

                    data = json.loads(message)
                    metadata = data.get("MetaData", {})
                    mmsi = metadata.get("MMSI")
                    ship_name = metadata.get("ShipName", "Unknown").strip()
                    
                    if mmsi:
                        unique_mmsis.add(mmsi)
                        ship_count += 1
                        
                        # 특정 중요한 데이터 타입만 로그 출력
                        msg_type = data.get("MessageType")
                        if msg_type == "PositionReport":
                            lat = metadata.get("latitude")
                            lon = metadata.get("longitude")
                            logger.info(f"[SHIP] {ship_name} (MMSI: {mmsi}) at Lat: {lat}, Lon: {lon}")

                logger.info(f"Streaming finished. Total messages: {ship_count}, Unique Ships: {len(unique_mmsis)}")
                return {
                    "total_messages": ship_count,
                    "unique_ships": len(unique_mmsis),
                    "ships_list": list(unique_mmsis)
                }

        except Exception as e:
            logger.error(f"Error during AIS streaming: {e}")
            raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    manager = AISManager()
    
    # 30초 동안 샘플 수집 테스트
    asyncio.run(manager.stream_hormuz_traffic(duration_sec=30))
