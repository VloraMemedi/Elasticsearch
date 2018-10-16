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
month  = 1454198400
year = 1482192000
threemonths = 1459468800


def avg_ai(param, start, end):
    time0 = time.time()
    avgai = csmoai.get_avg_param_val(param, start, end)
    time1 = time.time()
    dt = time1 - time0
    print("AI: Test avg in a day run seconds: ", dt, "value is:", avgai)

avgvalueai = avg_ai('NEI00008', start_date, day)


# # Scenario 2  - creating the index


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

def avg_el_sc2(index):
    time0= time.time()
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}], timeout=1000)
    jsondata = get_data_json('NEI00008', start_date, day)
    print("Data finished!")
    createindex = create_bulk_index(es, index=index, doc_type="_doc", data=jsondata)
    print("index created!")
    avg = es.search(index, body={"aggs": {"avg_value": {"avg": {"field": "value"}}}})
    valueavg = avg['aggregations']['avg_value']
    time1 =time.time()
    dt = time1-time0
    print('SC2: Test_min_three months run seconds', dt, "value is:", valueavg)


avg_el = avg_el_sc2(index="aday-avg")


# # Scenario 3


def avg_el_sc3(index):
    time0= time.time()
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}], timeout=1000)
    avg = es.search(index, body={"aggs": {"avg_value": {"avg": {"field": "value"}}}})
    valueavg = avg['aggregations']['avg_value']
    time1 =time.time()
    dt = time1-time0
    print('SC3: Test_avg_threemonths run seconds', dt, "value is:", valueavg)


avg_sc3 = avg_el_sc3(index="aday-avg")
