import csmoai
import csmodb
import requests
import json
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time
import pandas as pd

param = "NEI00008"
start_date = 1451606400
day = 1451692800
week = 1452124800
month  = 1454198400
year = 1482192000
threemonths = 1459468800
host = 'localhost'
port = 9200
doc_type ='_doc'


def maximum_ai(param, start, end):
    time0 = time.time()
    maximumai = csmoai.get_max_param_val(param, start, end)
    time1 = time.time()
    dt = time1 - time0
    print("AI: Test maximum day run seconds: ", dt, "value is:", maximumai)

maximumvalueai = maximum_ai(param, start_date, day)


#Scenario 2  - creating the index


def get_data_json(param, time_start, time_end):
    df = csmodb.get_ares_param_data_df([param], time_start, time_end)
    json_string = json.loads(df.to_json())[param]
    return json_string


def create_bulk_index(connection, index, doc_type, data):
    docs = []
    id = 1
    for key in data:
        # print("adding document with id:", id)
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


def maximum_el_sc2(index, host, port, param, start_date, end_date, doc_type):
    time0= time.time()
    es = Elasticsearch([{'host': host, 'port': port}], timeout=1000)
    jsondata = get_data_json(param, start_date, end_date)
    print("Data finished!")
    createindex = create_bulk_index(es, index=index, doc_type=doc_type, data=jsondata)
    print("index created!")
    maximum= es.search(index, body={"aggs": {"maximum_value": {"max": {"field": "value"}}}})
    valuemax = maximum['aggregations']['maximum_value']
    time1 =time.time()
    dt = time1-time0
    print("SC2: Test max day run seconds",  dt, "value is:", valuemax)
    # print("SC2: Test week run seconds", dt, "value is:", valuemax)
    # print("SC2: Test month run seconds", dt, "value is:", valuemax)


maximumel = maximum_el_sc2("aday-maximumtest", host, port, param, start_date, day, doc_type)


#Scenario 3


def maximum_el_sc3(index, host, port):
    time0= time.time()
    es = Elasticsearch([{'host': host, 'port': port}], timeout=1000)
    maximum = es.search(index, body={"aggs": {"maximum_value": {"max": {"field": "value"}}}})
    valuemax = maximum['aggregations']['maximum_value']
    time1 =time.time()
    dt = time1-time0
    print('SC3: Test max day run seconds', dt, "value is:", valuemax)
    # print("SC3: Test max week run seconds", dt, "value is:", valuemax)
    # print("SC3: Test  max month run seconds", dt, "value is:", valuemax)

maximum_sc3 = maximum_el_sc3("aday-maximumtest", host, port)
