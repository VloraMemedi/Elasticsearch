import csmoai
import csmodb
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import time
import pandas as pd
from collections import deque


param_enum = "NEI00008"
start_date = 1451606400
day = 1451692800
week = 1452124800
month  = 1454198400
year = 1482192000
threemonths = 1459468800

"Scenario 1 - CSMO Analytics"

def minimum_ai(param, start, end):
    minimumai = csmoai.get_min_param_val(param, start, end)


"Scenario 2 - Creating the index and storing data"


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
    # helpers.bulk(connection, docs)
    bulk = helpers.parallel_bulk(client=connection, actions=docs, thread_count=8, chunk_size=200)
    deque(bulk, maxlen=0)
    connection.indices.refresh()


def minimum_el_sc2(index, host, port, param_enum, start_date, end_date, doc_type):
    time0= time.time()
    es = Elasticsearch([{'host': host, 'port': port}], timeout=1000)
    jsondata = get_data_json(param_enum, start_date, end_date)
    print("Data finished!")
    createindex = create_bulk_index(es, index=index, doc_type=doc_type, data=jsondata)
    print("index created!")
    minimum = es.search(index, body={"aggs": {"minimum_value": {"min": {"field": "value"}}}})
    valuemin = minimum['aggregations']['minimum_value']
    time1 =time.time()
    dt = time1-time0


"Scenario 3 - Querying data with available index"


def minimum_el_sc3(index, host, port):
    time0= time.time()
    es = Elasticsearch([{'host': host, 'port': port}], timeout=1000)
    minimum = es.search(index, body={"aggs": {"minimum_value": {"min": {"field": "value"}}}})
    valuemin = minimum['aggregations']['minimum_value']
    time1 =time.time()
    dt = time1-time0



"Scenarios for the maximum calculations"

#Scenario 1


def maximum_ai(param, start, end):
    maximumai = csmoai.get_max_param_val(param, start, end)


#Scenario 2

def maximum_el_sc2(index, host, port, param_enum, start_date, end_date, doc_type):
    es = Elasticsearch([{'host': host, 'port': port}], timeout=1000)
    jsondata = get_data_json(param_enum, start_date, end_date)
    print("Data finished!")
    createindex = create_bulk_index(es, index=index, doc_type=doc_type, data=jsondata)
    print("index created!")
    maximum = es.search(index, body={"aggs": {"maximum_value": {"max": {"field": "value"}}}})
    valuemax = maximum['aggregations']['maximum_value']


#Scenario 3

def maximum_el_sc3(index, host, port):
    es = Elasticsearch([{'host': host, 'port': port}], timeout=1000)
    maximum = es.search(index, body={"aggs": {"maximum_value": {"max": {"field": "value"}}}})
    valuemax = maximum['aggregations']['maximum_value']
    es.indices.refresh(index=index)

"Scenarios for the  avg  calculations"

#Scenario 1

def avg_ai(param, start, end):
    avgai = csmoai.get_avg_param_val(param, start, end)


#Scenario 2

def avg_el_sc2(index, host, port, param_enum, start_date, end_date, doc_type):
    es = Elasticsearch([{'host': host, 'port': port}], timeout=1000)
    jsondata = get_data_json(param_enum, start_date, end_date)
    print("Data finished!")
    createindex = create_bulk_index(es, index=index, doc_type=doc_type, data=jsondata)
    print("index created!")
    avg = es.search(index, body={"aggs": {"average_value": {"avg": {"field": "value"}}}})
    valueavg = avg['aggregations']['average_value']


#Scenario 3

def avg_el_sc3(index, host, port):
    es = Elasticsearch([{'host': host, 'port': port}], timeout=1000)
    avg = es.search(index, body={"aggs": {"average_value": {"avg": {"field": "value"}}}})
    valueavg = avg['aggregations']['average_value']
    es.indices.refresh(index=index)
