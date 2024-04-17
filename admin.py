from confluent_kafka.admin import AdminClient, NewTopic
from sys import argv

topic1 = argv[1]
topic2 = argv[2]

a = AdminClient({'bootstrap.servers': 'localhost:9092'})


new_topics = [NewTopic(topic, num_partitions=2, replication_factor=1) for topic in [topic1, topic2]]

fs = a.create_topics(new_topics)

for topic, f in fs.items():
    try:
        f.result()  
        print("Topic {} created".format(topic))
    except Exception as e:
        print("Failed to create topic {}: {}".format(topic, e))