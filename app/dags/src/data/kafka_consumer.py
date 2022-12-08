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
        'data',                                # specify topic to consume from
        bootstrap_servers=[kafka_broker],
        consumer_timeout_ms=3000,                       # break connection if the consumer has fetched anything for 3 secs (e.g. in case of an empty topic)
        auto_offset_reset='earliest',                   # automatically reset the offset to the earliest offset (should the current offset be deleted or anything)
        enable_auto_commit=True,
        #value_deserializer=lambda x: loads(x.decode('utf-8'))
        value_deserializer=lambda x: deserialize(schema, x)
    )

    try:
        for message in consumer:
            #print(message.value)
            #print(type(message.value))
            df.append(message.value)
            #new = pd.DataFrame(message.value)
            #df.append(new)
        
        raw = pd.DataFrame(df)
        print(raw)
        raw.to_excel(open(os.getcwd()+kwargs['path_init']+"raw.xlsx", "wb"))
        
        #df1 = pd.concat(df, axis=0, ignore_index=True)
        #print(df1.to_markdown)
        #df1.to_excel(open(os.getcwd()+kwargs['path_init']+"raw.xlsx", "wb"))

    except Exception as e:
        print(e)
        logging.info('Error: '+e)


