import os
from dotenv import load_dotenv

load_dotenv()
import time

import requests
from google.transit import gtfs_realtime_pb2


API_KEY = os.getenv("VALLEY_METRO_API_KEY")

if not API_KEY:
    raise RuntimeError("VALLEY_METRO_API_KEY is not configured")

URL = (
    "https://mna.mecatran.com/utw/ws/gtfsfeed/"
    f"vehicles/valleymetro?apiKey={API_KEY}"
)

def fetch_vehicle_data():

    response = requests.get(URL, timeout=10)

    response.raise_for_status()

    feed = gtfs_realtime_pb2.FeedMessage()

    feed.ParseFromString(response.content)

    vehicles = []

    for entity in feed.entity:

        if not entity.HasField("vehicle"):
            continue

        vehicle = entity.vehicle

        vehicle_data = {
            "vehicle_id": vehicle.vehicle.id,
            "trip_id": vehicle.trip.trip_id or None,
            "route_id": vehicle.trip.route_id or None,
            "latitude": vehicle.position.latitude,
            "longitude": vehicle.position.longitude,
            "timestamp": vehicle.timestamp
        }

        vehicles.append(vehicle_data)

    return vehicles


if __name__ == "__main__":

    while True:

        vehicles = fetch_vehicle_data()

        print("\nVehicles received:", len(vehicles))

        for vehicle in vehicles[:5]:
            print(vehicle)

        print("-" * 80)

        time.sleep(15)