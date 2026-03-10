import asyncio, websockets, json, os
from datetime import datetime, timezone

async def connect_ais_stream():
    url = "wss://stream.aisstream.io/v0/stream"
    try:
        # 타임아웃 60초로 극대화
        async with websockets.connect(url, open_timeout=60) as websocket:
            # Luzon Strait BBOX: [[19.9889, 120.8523], [20.9889, 121.8523]]
            subscribe_message = {
                "APIKey": "6aa66f6f8daa2d33fbe6c6bf05968cb3b13c83e9", 
                "BoundingBoxes": [[[19.0, 118.0], [22.0, 124.0]]] # 좀 더 넉넉하게 잡음
            }

            await websocket.send(json.dumps(subscribe_message))
            print("--- Subscribed to Luzon Strait. Waiting for data... ---")

            count = 0
            async for message_json in websocket:
                message = json.loads(message_json)
                if message["MessageType"] == "PositionReport":
                    ais_message = message['Message']['PositionReport']
                    ship_name = message['MetaData'].get('ShipName', 'Unknown')
                    print(f"[{datetime.now(timezone.utc)}] Ship: {ship_name} MMSI: {ais_message['UserID']} Lat: {ais_message['Latitude']} Lon: {ais_message['Longitude']}")
                    count += 1
                    if count >= 3: break
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(connect_ais_stream())
