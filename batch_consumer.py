from kafka import KafkaConsumer
import boto3 
import json
import datetime 

currentDate = datetime.date.today()

folder = currentDate.strftime("%Y-%m-%d")

consumer = KafkaConsumer("MyFirstKafkaTopic", bootstrap_servers=['localhost:9092'], api_version=(0, 10, 2)) #, consumer_timeout_ms=1000000)
s3_client = boto3.client('s3')
session = boto3.Session()
s3h = session.resource('s3')
bucket = s3h.Bucket('pinterest-data-6caaf6b1-2aef-4376-93c5-e713a3717d92')

count = 0
batch = []
for msg in consumer:
    batch.append(msg.value)
    if len(batch) > 0:
        data = json.loads(msg.value)
        count = count + 1
        key_json = [folder, '/', count, ".json"]
        #k = bucket.new_key(''.join(str(key_json)))
        #k.set_contents_from_string('')

        json_object = data
        print("".join(str(x) for x in key_json))
       # print(k)


        s3_client.put_object(Body=json.dumps(json_object), Bucket='pinterest-data-6caaf6b1-2aef-4376-93c5-e713a3717d92', Key="".join(str(x) for x in key_json))
consumer.close()

