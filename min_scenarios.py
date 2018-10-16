import csmoai
import csmodb
import requests
import json
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time
import pandas as pd

param_enum = "NEI00008"
start_date = 1451606400
day = 1451692800
week = 1452124800
month  = 1454198400
year = 1482192000
threemonths = 1459468800


def minimum_ai(param, start, end):
    time0 = time.time()
    minimumai = csmoai.get_min_param_val(param, start, end)
    time1 = time.time()
    dt = time1 - time0
    print("AI: Test_min_year run seconds: ", dt, "value is:", minimumai)

minimumvalueai = minimum_ai(param_enum, start_date, day)


# # Scenario 2  - creating the index


def get_data_json(param, time_start, time_end):
    df = csmodb.get_ares_param_data_df([param], time_start, time_end)
    json_string = json.loads(df.to_json())[param]
    return json_string



def create_bulk_index(connection, index, doc_type, data):
    docs = []
    id = 1
    for key in data:
        print("adding document with id:", id)
        date = pd.to_datetime(key, unit='us')
        docs.append(
            {
                "_index": index,
                "_type": doc_type,
                "_id": id,
                "_source": {
                    "timestamp": date,
                    "value": data[key]
                }
            }
        )
        id += 1
    helpers.bulk(connection, docs)

def minimum_el_sc2(index):
    time0= time.time()
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}], timeout=1000)
    jsondata = get_data_json(param_enum, start_date, day)
    print("Data finished!")
    # settings = es.indices.put_settings(body={"settings": {"refresh_interval": "-1"}})
    createindex = create_bulk_index(es, index=index, doc_type="_doc", data=jsondata)
    print("index created!")
    minimum = es.search(index, body={"aggs": {"minimum_value": {"min": {"field": "value"}}}})
    valuemin = minimum['aggregations']['minimum_value']
    time1 =time.time()
    dt = time1-time0
    print("SC2: Test day rune seoonds is ", dt, "value is:", valuemin)


minimumel = minimum_el_sc2(index="aday-min")


# # Scenario 3


def minimum_el_sc3(index):
    time0= time.time()
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}], timeout=1000)
    minimum = es.search(index, body={"aggs": {"minimum_value": {"min": {"field": "value"}}}})
    valuemin = minimum['aggregations']['minimum_value']
    time1 =time.time()
    dt = time1-time0
    print('SC3: Test_min_threemonths run seconds', dt, "value is:", valuemin)


minimumel_sc3 = minimum_el_sc3(index="aday-min")
