import argparse

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
import csv

API_KEY = 'UUIWRCTC4HGPDO6D'
ENDPOINT_SCHEMA_URL  = 'https://psrc-30dr2.us-central1.gcp.confluent.cloud'
API_SECRET_KEY = 'oXNMVNuNHZwd2ziDncoLvcyZowU7YUGDgpjSskicv5lZEKn8SOiQFPV8Z+/hz5Ca'
BOOTSTRAP_SERVER = 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = '5M5NBJLR77ARYVHL'
SCHEMA_REGISTRY_API_SECRET = 'WgD03K9LaGsfW1QaVh+IqDxQG8guVIEJ3BMKO6q5+f7AMzDNeWTB7MjimweEVYpu'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Restaurent:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_restaurent(data:dict,ctx):
        return Restaurent(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)  #sc
    schema_name = schema_registry_client.get_schema(100004).schema_str
    # string_serializer = StringSerializer('utf_8')
    # json_deserializer = JSONDeserializer(schema_str,
    #                                      from_dict=Car.dict_to_car)
    json_deserializer = JSONDeserializer(schema_name,
                                         from_dict=Restaurent.dict_to_restaurent)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'Dev',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    data = []
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            restaurent = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if restaurent is not None:
                restaurant_data = restaurent.__dict__
                print(restaurant_data)
                with open('restaurant_data.csv', 'w', buffering=5*(1024**2)) as myFile:
                    writer = csv.DictWriter(myFile, fieldnames=list(restaurant_data.keys()))
                    writer.writerow(restaurant_data)

        except KeyboardInterrupt:
            break

    consumer.close()

main("my-resto")
