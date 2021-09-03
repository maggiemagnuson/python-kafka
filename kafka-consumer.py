from kafka import KafkaConsumer
import json

def consume_messages():
    consumer = KafkaConsumer('<TOPIC_HERE>',
                             bootstrap_servers=['<KAFKA_HOST_AND_PORT_HERE>'],
                             security_protocol='SSL',
                             ssl_check_hostname=False,
                             ssl_certfile="certificate.pem",
                             ssl_keyfile="key.pem",
                             #ssl_cafile=s["ssl_cafile"],
                             ssl_password="",
                             group_id='<GROUP_ID_HERE>',
                             auto_offset_reset='latest',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    print("starting consumption")

    for msg in consumer:
        print(msg)

if __name__ == '__main__':
    consume_messages()
