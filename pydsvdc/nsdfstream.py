from kafka import KafkaConsumer
import json

def get_kafka_conn_str():
    kafka_sock = '54.145.37.197:9092'
    return(kafka_sock)

class NSDFEventStream:
    def __init__(self, streamnane):
        topic = streamname
        conn_str = get_kafka_conn_str()
        self.consumer =  KafkaConsumer(topic, conn_str, value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    def __iter__(self):
        self.kiter = iter(self.consumer)
        return(self)

    def __next__(self):
        while true:
            message = next(self.kiter)
            print(message)



