import unittest
from validation.validate_vehicle import validate_vehicle


class TestVehicleValidation(unittest.TestCase):

    def setUp(self):
        self.vehicle = {
            "vehicle_id": "BUS_101",
            "latitude": 33.4255,
            "longitude": -111.9400,
            "timestamp": 1793400000,
        }

    def test_valid_vehicle(self):
        self.assertEqual(validate_vehicle(self.vehicle), [])

    def test_missing_vehicle_id(self):
        v = {**self.vehicle, "vehicle_id": ""}
        self.assertEqual(validate_vehicle(v), ["missing_vehicle_id"])

    def test_missing_latitude(self):
        v = {**self.vehicle, "latitude": None}
        self.assertEqual(validate_vehicle(v), ["missing_latitude"])

    def test_invalid_latitude(self):
        v = {**self.vehicle, "latitude": 200}
        self.assertEqual(validate_vehicle(v), ["invalid_latitude"])

    def test_missing_longitude(self):
        v = {**self.vehicle, "longitude": None}
        self.assertEqual(validate_vehicle(v), ["missing_longitude"])

    def test_invalid_longitude(self):
        v = {**self.vehicle, "longitude": -200}
        self.assertEqual(validate_vehicle(v), ["invalid_longitude"])

    def test_missing_timestamp(self):
        v = {**self.vehicle, "timestamp": None}
        self.assertEqual(validate_vehicle(v), ["missing_timestamp"])

    def test_multiple_errors(self):
        v = {
            "vehicle_id": "",
            "latitude": 200,
            "longitude": -200,
            "timestamp": None,
        }
        self.assertEqual(
            validate_vehicle(v),
            [
                "missing_vehicle_id",
                "invalid_latitude",
                "invalid_longitude",
                "missing_timestamp",
            ],
        )

    def test_coordinate_boundaries(self):
        for lat in (-90, 90):
            for lon in (-180, 180):
                with self.subTest(lat=lat, lon=lon):
                    v = {**self.vehicle, "latitude": lat, "longitude": lon}
                    self.assertEqual(validate_vehicle(v), [])


if __name__ == "__main__":
    unittest.main()
