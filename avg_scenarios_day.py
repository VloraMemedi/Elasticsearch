# Scenario 1
import csmoai
import csmodb
import json
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time
import pandas as pd
start_date = 1451606400
day = 1451692800
week = 1452124800
month = 1454198400
year = 1482192000
threemonths = 1459468800
host = 'localhost'
port = 9200
param = 'NEI00008'


#Scenario 1 - CSMO Analytics

def avg_ai(param, start, end):
    time0 = time.time()
    avgai = csmoai.get_avg_param_val(param, start, end)
    time1 = time.time()
    dt = time1 - time0
    print("AI: Test avg in a day run seconds: ", dt, "value is:", avgai)
    # print("AI: Test avg in a week run seconds: ", dt, "value is:", avgai)
    # print("AI: Test avg in a year run seconds: ", dt, "value is:", avgai)


avgai = avg_ai(param, start_date, day)

#Scenario 2  - creating the bulk index


def get_data_json(param, time_start, time_end):
    df = csmodb.get_ares_param_data_df([param], time_start, time_end)
    json_string = json.loads(df.to_json())[param]
    return json_string



def create_bulk_index(connection, index, doc_type, data):
    docs = []
    id = 1
    for key in data:
        # print("adding id:", id)
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


def avg_el_sc2(index, host, port, param, start_date, end_date):
    time0= time.time()
    es = Elasticsearch([{'host': host, 'port': port}], timeout=1000)
    jsondata = get_data_json(param, start_date, end_date)
    print("Data finished!")
    createindex = create_bulk_index(es, index=index, doc_type="_doc", data=jsondata)
    print("index created!")
    avg = es.search(index, body={"aggs": {"avg_value": {"avg": {"field": "value"}}}})
    valueavg = avg['aggregations']['avg_value']
    time1 =time.time()
    dt = time1-time0
    print('SC2: Test avg in a day months run seconds', dt, "value is:", valueavg)
    # print('SC2: Test avg in a week run seconds', dt, "value is:", valueavg)
    # print('SC2: Test avg in a month run seconds', dt, "value is:", valueavg)


avg_el = avg_el_sc2("aday-avgtest10", host, port, param, start_date, day)


# Scenario 3 - querying on stored data


def avg_el_sc3(index, host, port):
    time0= time.time()
    es = Elasticsearch([{'host': host, 'port': port}], timeout=1000)
    avg = es.search(index, body={"aggs": {"avg_value": {"avg": {"field": "value"}}}})
    valueavg = avg['aggregations']['avg_value']
    time1 =time.time()
    dt = time1-time0
    print('SC3: Test for a day run seconds', dt, "value is:", valueavg)
    # print('SC3: Test for a week run seconds', dt, "value is:", valueavg)
    # print('SC3: Test for a year run seconds', dt, "value is:", valueavg)


avg_sc3 = avg_el_sc3("aday-avgtest10", host, port)
