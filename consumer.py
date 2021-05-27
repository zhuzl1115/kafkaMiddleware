from kafka import KafkaConsumer
from pykafka import KafkaClient
import json
import msgpack
"""
待解决问题：

1）consumer必须先开启，无法接受开启之前producer发送的消息

"""
class Consumer:
    def __init__(self, topic, groupId, service = ['localhost:9092']):
        self.consumer = KafkaConsumer (
            topic,
            group_id = str(groupId),
            bootstrap_servers = service
        )
        print(self.consumer.partitions_for_topic(topic))

    def recv(self):
        ret = []
        for message in self.consumer:
            key = json.loads(message.key.decode())
            value = json.loads(message.value.decode())
            print("receive, key: {}, value: {}".format (key, value))
            ret.append({key: key, value: value})
        return ret 

if __name__ == '__main__':
    consumer = Consumer(topic='newTopic', groupId=0)
    consumer.recv()


