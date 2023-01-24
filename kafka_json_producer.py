#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# A simple example demonstrating use of JSONSerializer.

import argparse
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
#from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List

FILE_PATH = "restaurant_orders.csv"
columns=['Order Number', 'Order Date', 'Item Name', 'Quantity', 'Product Price', 'Total products']

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
                 # 'security.protocol': 'SASL_PLAINTEXT'}
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


def get_restaurent_instance(file_path):
    df=pd.read_csv(file_path)
    df['Item Name'] = df['Item Name']
    df['Quantity'] = df['Quantity'].astype(str)
    df['Product Price'] = df['Product Price'].astype(str)
    df['Total products'] = df['Total products'].astype(str)
    df=df.iloc[:,1:]
    restaurents:List[Restaurent]=[]
    for data in df.values:
        restaurent=Restaurent(dict(zip(columns,data)))
        restaurents.append(restaurent)
        yield restaurent

def restaurent_to_dict(restaurent:Restaurent, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return restaurent.record

def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

def main(topic):

    schema_str = """
    {
  "$id": "http://example.com/myURI.schema.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "additionalProperties": false,
  "description": "Sample schema to help you get started.",
  "properties": {
    "Order Number": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "Order Date": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "Item Name": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "Quantity": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "Product Price": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "Total products": {
      "description": "The type(v) type is used.",
      "type": "string"
    }
  },
  "title": "SampleRecord",
  "type": "object"
}
    """
    schema_registry_conf = schema_config()
    # schema_registry_conf.get_latest_schema()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)  #sc
    schema_name = schema_registry_client.get_schema(100003).schema_str
    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_name, schema_registry_client, restaurent_to_dict)

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))

    producer.poll(0.0)
    try:
        for restaurent in get_restaurent_instance(file_path=FILE_PATH):

            print(restaurent)
            producer.produce(topic=topic,
                            key=string_serializer(str(uuid4()), restaurent_to_dict),
                            value=json_serializer(restaurent, SerializationContext(topic, MessageField.VALUE)),
                            on_delivery=delivery_report)
            # break
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()

main("my-resto")


