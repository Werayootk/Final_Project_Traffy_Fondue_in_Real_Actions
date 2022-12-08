from kafka import KafkaProducer
import requests
from json import dumps
import json
import avro.schema
import avro.io
import io
import os

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
        #value_serializer=lambda x: dumps(x).encode('utf-8'),

    )
    api_url = 'https://publicapi.traffy.in.th/share/teamchadchart/search?limit={}&offset={}'.format(start_limit, offset_limit)
    data_info = requests.get(api_url)
    set_info = json.loads(data_info.text) # set_info['total'] = 187616
    # data_length = set_info['total']
    data_length = 2000

    for offset_limit in range(0,data_length,1000):
        url = 'https://publicapi.traffy.in.th/share/teamchadchart/search?limit=1000&offset='+str(offset_limit)
        response_API = requests.get(url)
        info = json.loads(response_API.text)
        #producer.send('data', value=info)
        for data in info['results']:
             data_convert = serialize(schema, data)
             producer.send('data', value=data_convert)
    producer.close()
