from kafka import KafkaConsumer
from json import loads
import json
import logging
import pandas as pd
import numpy as np
import os
import avro.schema
import avro.io
import io

def deserialize(schema, raw_bytes):
    bytes_reader = io.BytesIO(raw_bytes)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    return reader.read(decoder)

def get_data_init(**kwargs):
    schema_file = os.getcwd()+kwargs['path_avsc']+"schema.avsc"
    schema = avro.schema.parse(open(schema_file).read())
    df = []
    kafka_broker = 'kafka:9092'
    consumer = KafkaConsumer(
        'data',                                
        bootstrap_servers=[kafka_broker],
        consumer_timeout_ms=3000,                       
        auto_offset_reset='earliest',                   
        enable_auto_commit=True,
        value_deserializer=lambda x: deserialize(schema, x)
    )

    try:
        for message in consumer:
            #print(message.value)
            #print(type(message.value))
            df.append(message.value)
        
        raw = pd.DataFrame(df)
        #print(raw)
        raw.to_excel(open(os.getcwd()+kwargs['path_init']+"raw.xlsx", "wb"))

    except Exception as e:
        print(e)
        logging.info('Error: '+e)

def update_data_daily(**kwargs):
    schema_file = os.getcwd()+kwargs['path_avsc']+"schema.avsc"
    schema = avro.schema.parse(open(schema_file).read())
    df = []
    kafka_broker = 'kafka:9092'
    consumer = KafkaConsumer(
        'data',                                
        bootstrap_servers=[kafka_broker],
        consumer_timeout_ms=3000,                       
        auto_offset_reset='earliest',                   
        enable_auto_commit=True,
        value_deserializer=lambda x: deserialize(schema, x)
    )

    try:
        for message in consumer:
            #print(message.value)
            #print(type(message.value))
            df.append(message.value)
        
        raw = pd.DataFrame(df)
        #print(raw)
        raw.to_excel(open(os.getcwd()+kwargs['path_init']+"raw.xlsx", "wb"))

    except Exception as e:
        print(e)
        logging.info('Error: '+e)

