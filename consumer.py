from confluent_kafka import Consumer
from sys import argv

group_id = argv[1]
topic = argv[2]

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'group_id',
    'auto.offset.reset': 'earliest'
})

c.subscribe([topic])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()