import asyncio
import websockets
import json
from datetime import datetime, timezone

async def connect_ais_stream():
    try:
        async with websockets.connect("wss://stream.aisstream.io/v0/stream", open_timeout=30) as websocket:
            # Suez Canal BBOX: [[30.0933, 31.9369], [31.0933, 32.9369]]
            subscribe_message = {
                "APIKey": "6aa66f6f8daa2d33fbe6c6bf05968cb3b13c83e9", 
                "BoundingBoxes": [[[30.0, 31.5], [31.5, 33.0]]]
            }

            await websocket.send(json.dumps(subscribe_message))
            print("--- Subscribed to Suez Canal. Waiting for real-time data... ---")

            # 데이터 3개만 받으면 종료
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
