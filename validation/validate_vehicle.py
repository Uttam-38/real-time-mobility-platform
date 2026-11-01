def validate_vehicle(vehicle):

    errors = []

    # Vehicle ID must exist
    if not vehicle.get("vehicle_id"):
        errors.append("missing_vehicle_id")

    # Latitude must exist and be valid
    latitude = vehicle.get("latitude")

    if latitude is None:
        errors.append("missing_latitude")
    elif not -90 <= latitude <= 90:
        errors.append("invalid_latitude")

    # Longitude must exist and be valid
    longitude = vehicle.get("longitude")

    if longitude is None:
        errors.append("missing_longitude")
    elif not -180 <= longitude <= 180:
        errors.append("invalid_longitude")

    # Timestamp must exist
    if not vehicle.get("timestamp"):
        errors.append("missing_timestamp")

    return errors