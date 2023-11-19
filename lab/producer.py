from confluent_kafka import Producer
from faker import Faker
import json
from time import sleep
import uuid
import random



fake = Faker()

motor_spare_parts = [
    'Oil Filter', 'Brake Pad', 'Spark Plug', 'Alternator', 'Air Filter',
    'Timing Belt', 'Water Pump', 'Fuel Pump', 'Radiator', 'Ignition Coil',
    'Drive Belt', 'Clutch Kit', 'Throttle Body', 'Oxygen Sensor', 'Starter Motor',
    'CV Joint', 'Power Steering Pump', 'Wheel Bearing', 'Headlight Bulb', 'Battery',
    'Shock Absorber', 'Strut Assembly', 'Tie Rod End', 'Control Arm', 'Catalytic Converter',
    'Exhaust Pipe', 'Fuel Injector', 'Transmission Mount', 'Engine Mount', 'Wheel Hub Assembly',
    'Brake Caliper', 'Wheel Cylinder', 'Fuel Tank', 'Windshield Wiper Blade', 'Spark Plug Wire Set',
    'Distributor Cap', 'Fuel Filter', 'Air Conditioning Compressor', 'Brake Master Cylinder', 'Ignition Control Module',
    'EGR Valve', 'Mass Air Flow Sensor', 'PCV Valve', 'Transmission Filter', 'Cabin Air Filter',
    'Wheel Seal', 'Axle Shaft', 'Transmission Control Module', 'Engine Control Module', 'MAP Sensor',
    'Ball Joint', 'Radiator Fan Assembly', 'Serpentine Belt', 'Thermostat', 'Camshaft Position Sensor',
    'Crankshaft Position Sensor', 'Engine Oil Cooler', 'Turbocharger', 'Intercooler', 'Suspension Strut and Coil Spring Assembly'
]


producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all',
    'delivery.timeout.ms': 10000
})

class DataGenerator(object):

    @staticmethod
    def get_data():
        current_time = fake.date_time_this_year()
        day_of_week = current_time.strftime("%A")  # Get the day of the week (Monday to Sunday)
        return {
            "transaction_id": str(uuid.uuid4()),
            "product_name": random.choice(motor_spare_parts),
            "category": fake.random_element(elements=('Engine Parts', 'Brake System', 'Suspension', 'Electrical', 'Transmission')),
            "quantity": fake.random_int(min=1, max=10),
            "unit_price": fake.random_int(min=10, max=100),
            "timestamp": int(current_time.timestamp()),
            "day_of_week": day_of_week,
            "month_of_year": current_time.month
        }

# Generate and produce data continuously
while True:
    data = DataGenerator.get_data()
    _payload = json.dumps(data).encode("utf-8")
    producer.produce('Sales', value=_payload)
    print(_payload)
    sleep(5)
    sleep(5)