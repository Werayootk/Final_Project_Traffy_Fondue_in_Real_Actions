from kafka import KafkaProducer
import requests
from json import dumps
import json
import avro.schema
import avro.io
import io
import os
from datetime import date
from datetime import timedelta

def serialize(schema, obj):
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer = avro.io.DatumWriter(schema)
    writer.write(obj, encoder)
    return bytes_writer.getvalue()

def load_data_init(**kwargs):
    schema_file = os.getcwd()+kwargs['path_avsc']+"schema.avsc"
    schema = avro.schema.parse(open(schema_file).read())
    start_limit = 1000
    offset_limit = 0
    kafka_broker = 'kafka:9092'

    producer = KafkaProducer(
        bootstrap_servers=[kafka_broker],
    )
    api_url = 'https://publicapi.traffy.in.th/share/teamchadchart/search?limit={}&offset={}'.format(start_limit, offset_limit)
    data_info = requests.get(api_url)
    set_info = json.loads(data_info.text)
    data_length = set_info['total']
    # data_length = 2000

    for offset_limit in range(0,data_length,1000):
        url = 'https://publicapi.traffy.in.th/share/teamchadchart/search?limit=1000&offset='+str(offset_limit)
        response_API = requests.get(url)
        info = json.loads(response_API.text)
        for data in info['results']:
             data_convert = serialize(schema, data)
             producer.send('data', value=data_convert)
    producer.close()

def load_data_daily(**kwargs):
    schema_file = os.getcwd()+kwargs['path_schema']+"schema.avsc"
    schema = avro.schema.parse(open(schema_file).read())
    kafka_broker = 'kafka:9092'

    producer = KafkaProducer(
        bootstrap_servers=[kafka_broker],
    )

    # Get today's date
    today = date.today()
    print("Today is: ", today) # 2019-12-11
    
    # Yesterday date
    yesterday = today - timedelta(days = 1)
    print("Yesterday was: ", yesterday) # 2019-12-10

    api_url = 'https://publicapi.traffy.in.th/share/teamchadchart/search?limit=1000&offset=0&start={}&end={}'.format(yesterday, today)
    data_info = requests.get(api_url)
    set_info = json.loads(data_info.text)
    data_length = set_info['total']

    for offset_limit in range(0,data_length,1000):
        url = 'https://publicapi.traffy.in.th/share/teamchadchart/search?limit=1000&offset='+str(offset_limit)
        response_API = requests.get(url)
        info = json.loads(response_API.text)
        for data in info['results']:
             data_convert = serialize(schema, data)
             producer.send('data', value=data_convert)
    producer.close()