from confluent_kafka import Producer
from sys import argv
from datetime import datetime
import json


driver_name = argv[1]
driver_location = argv[2]

driverinfo_data =[{
    "driver_id": "DR0001",
    "name": driver_name,
    "vehicle": "Toyota Prius",
    "location": driver_location,
}]
foodInfo_data = [{
    "driver_id": "DR0001",
    "date": datetime.now().strftime("%Y-%m-%d"),
    "cost": 10,
    "rider_id": "RD0003",
    "rating": 3
}]

p = Producer({
    'bootstrap.servers': 'localhost:9092',
    'linger.ms': 100,  # Wait up to 100ms before sending a batch
    'batch.size': 32 * 1024  # Send batches of up to 32KB
})
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

partition = 0 if driver_location =="dharan" else 1

p.produce('driver_info', partition=partition, value=json.dumps(driverinfo_data), callback=delivery_report)
p.produce('food_info', partition=partition, value=json.dumps(foodInfo_data), callback=delivery_report)

p.flush()
