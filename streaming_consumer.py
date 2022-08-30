from kafka import KafkaConsumer
from json import loads
consumer = KafkaConsumer("MyFirstKafkaTopic",bootstrap_servers=['localhost:9092'] ,api_version=(0,10,2))
# consumer = KafkaConsumer(
#     'numtest',
#      bootstrap_servers=['localhost:9092'],
#      auto_offset_reset='earliest',
#      enable_auto_commit=True,
#      group_id='my-group',
#      value_deserializer=lambda x: loads(x.decode('utf-8')),api_version=(0,10,2))


#msg = next(consumer)

for msg in consumer:
    print(msg.value)