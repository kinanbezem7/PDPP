from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],api_version=(0,10,2))
for _ in range(100):
    producer.send('MyFirstKafkaTopic', b'some_message_bytes')