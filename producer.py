from kafka import KafkaProducer, KafkaConsumer
from pykafka import KafkaClient
from kafka.errors import kafka_errors
import traceback
import json
import argparse

class Producer:
    def __init__(self, service = ['localhost:9092']):
        self.producer = KafkaProducer (
                bootstrap_servers = service,
                key_serializer=lambda k: json.dumps(k).encode(),
                value_serializer=lambda v: json.dumps(v).encode()
            )
        self.client = KafkaClient(hosts="localhost:9092")
        print(self.client.brokers)
    
    def send(self, topic, data, key, partition):
        future = self.producer.send(
                topic,
                key = key,  # 同一个key值，会被送至同一个分区
                value = data,
                partition = partition
            )
        try:
            future.get(timeout=20) # 监控是否发送成功           
        except kafka_errors:  # 发送失败抛出kafka_errors
            traceback.format_exc()
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic', type=str)
    # parser.add_argument('--data', type=str)
    parser.add_argument('--key', type=str)
    parser.add_argument('--partition', type=int)
    args = parser.parse_args()
    with open('test.json', 'r') as f:
        data = f.read()
    produce = Producer()
    produce.send(topic=args.topic, data=data, key=args.key, partition=args.partition)




