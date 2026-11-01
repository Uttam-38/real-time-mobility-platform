import json

from kafka import KafkaConsumer


TOPIC_NAME = "vehicle-positions"


consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    )
)


print("Listening for vehicle-position messages...")


for message in consumer:

    vehicle = message.value

    print("Vehicle ID:", vehicle.get("vehicle_id"))
    print("Trip ID:", vehicle.get("trip_id"))
    print("Route ID:", vehicle.get("route_id"))
    print("Latitude:", vehicle.get("latitude"))
    print("Longitude:", vehicle.get("longitude"))
    print("Event Timestamp:", vehicle.get("timestamp"))
    print(
        "Ingestion Timestamp:",
        vehicle.get("ingestion_timestamp")
    )
    print("Source:", vehicle.get("source"))

    print("-" * 50)