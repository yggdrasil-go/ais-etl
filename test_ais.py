import asyncio
import websockets
import json
from datetime import datetime, timezone

async def connect_ais_stream():
    try:
        async with websockets.connect("wss://stream.aisstream.io/v0/stream", open_timeout=30) as websocket:
            subscribe_message = {
                "APIKey": "6aa66f6f8daa2d33fbe6c6bf05968cb3b13c83e9", 
                "BoundingBoxes": [[[-11, 178], [30, 74]]]
            }

            await websocket.send(json.dumps(subscribe_message))
            print("--- Subscription sent. Waiting for data... ---")

            # 30초만 실행하도록 루프에 타임아웃 추가
            async for message_json in asyncio.wait_for(websocket, timeout=30):
                message = json.loads(message_json)
                message_type = message["MessageType"]

                if message_type == "PositionReport":
                    ais_message = message['Message']['PositionReport']
                    print(f"[{datetime.now(timezone.utc)}] ShipId: {ais_message['UserID']} Latitude: {ais_message['Latitude']} Longitude: {ais_message['Longitude']}")
    except asyncio.TimeoutError:
        print("--- 30s timeout reached. Ending test. ---")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(connect_ais_stream())
