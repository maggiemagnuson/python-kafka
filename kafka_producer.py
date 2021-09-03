from kafka import KafkaProducer
from datetime import datetime, timedelta
import json
from random_word import RandomWords
import logging

logger = logging.getLogger("kafka-producer")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

def produce_messages_by_time(time):
    r = RandomWords()
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    while datetime.now() < time:
        try:
            word = r.get_random_word()
            producer.send("word-count", {"word": f"{word}", "event_time": f"{datetime.now()}"})
        except :
            print(f"failed when trying to publish: {word}")


def produce_messages():
    produce_messages_by_time(datetime.now() + timedelta(seconds=120))


if __name__ == '__main__':
    produce_messages()