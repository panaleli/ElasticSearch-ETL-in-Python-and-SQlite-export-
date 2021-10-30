#!/usr/bin/env python
# coding: utf-8

# In[1]:


#load libraries
from pandasticsearch.client import RestClient
from pandasticsearch.queries import Agg, Select
from pandasticsearch.operators import *
from pandasticsearch.types import Column
from pandasticsearch.types import Row
from elasticsearch import Elasticsearch
from pandasticsearch import Select
from pandasticsearch import DataFrame
from pandas import DataFrame
import numpy as np
import pandas as pd
import json
from os import environ
from espandas import Espandas
from pandas import DataFrame, Series
import sqlite3 as db
import requests
import json
import json, time
from pandas.io.json import json_normalize


# In[2]:


# declare globals for the Elasticsearch client host
DOMAIN = "localhost"
PORT = 9200
es_client = Elasticsearch() # localhost


# In[3]:


try:
# use the JSON library's dump() method for indentation
    info = json.dumps(es_client.info(), indent=4)

# pass client object to info() method
    print ("Elasticsearch client info():", info)

except exceptions.ConnectionError as err:

# print ConnectionError for Elasticsearch
    print ("nElasticsearch info() ERROR:", err)
    print ("nThe client host:", host, "is invalid or cluster is not running")

# change the client's value to 'None' if ConnectionError
    es_client = None


# In[4]:


#connection to elasticsearch (localhost). two indeces connection with scroll time 1 minute
elastic_url_1 = 'http://localhost:9200/cdr_1_small/_search?scroll=1m'
elastic_url_2 = 'http://localhost:9200/cdr_2_small/_search?scroll=1m'

scroll_api_url = 'http://localhost:9200/_search/scroll'
headers = {'Content-Type': 'application/json'}


# In[5]:


#filter search in DSL Elasticsearch

# select data with countrycode = 0
#payload = {
#    "size": 100,
#    "query": {
#        "match" : {
#            "countrycode" : "0"
#        }
#    }
#}

# select all data from ES index with batch size 100
payload = {
        "size": 100,
        "query": {
            "match_all": {}
        }
    }


# In[6]:


##Data retrive for 1st Index##


# In[7]:


r1 = requests.request(
    "POST",
    elastic_url_1,
    data=json.dumps(payload),
    headers=headers)


# In[8]:


# first batch data for 1st index
try:
    res_json = r1.json()
    data = res_json['hits']['hits']
    _scroll_id = res_json['_scroll_id']

except KeyError:
    data = []
    _scroll_id = None
    print ('Error: Elastic Search: %s' % str(r1.json()))


# In[10]:


#loop and retrive data from ES and store them in an SQLite DB
while data:

    #print (data)
      
    # scroll to get next batch data
    scroll_payload = json.dumps({
    'scroll': '1m',
    'scroll_id': _scroll_id
    })
        
    scroll_res = requests.request(
    "POST", scroll_api_url,
    data=scroll_payload,
    headers=headers
    )
    
    #normalize data from raw json to columns
    result_dict_1 = json_normalize(data)
    
    #reform a specific dataset 
    dataset = DataFrame(result_dict_1,columns=['_index','_id','_type','_source.@timestamp','_source.CellID','_source.callin','_source.callout',
                                          '_source.countrycode','_source.datetime','_source.smsin','_source.smsout',
                                           '_source.internet'])
    
    #rename columns
    dataset.columns=['_index','_id','_type','timestamp','CellID','callin','callout','countrycode','datetime',
                     'smsin','smsout','internet']
    
    #print(dataset)
    
    #connect to sqlite database
    connex = db.connect("store_4.db") 
    cur = connex.cursor() 
    
    #export dataset to sqlite
    dataset.to_sql(name="cdr_dataset", con=connex, if_exists="append", index=True)   

    #scroll data with unique id (avoid duplicates)           
    try:
        res_json = scroll_res.json()
        data = res_json['hits']['hits']
        _scroll_id = res_json['_scroll_id']

    except KeyError:
        data = []
        _scroll_id = None
        err_msg = 'Error: Elastic Search Scroll: %s'
        print (err_msg % str(scroll_res.json()))


# In[11]:


##Data retrive for 2nd Index##


# In[12]:


r2 = requests.request(
    "POST",
    elastic_url_2,
    data=json.dumps(payload),
    headers=headers)


# In[13]:


# first batch data for 2nd index
try:
    res_json = r2.json()
    data = res_json['hits']['hits']
    _scroll_id = res_json['_scroll_id']

except KeyError:
    data = []
    _scroll_id = None
    print ('Error: Elastic Search: %s' % str(r2.json()))


# In[15]:


#loop and retrive data from ES and store them in an SQLite DB
while data:

    #print (data)
      
    # scroll to get next batch data
    scroll_payload = json.dumps({
    'scroll': '1m',
    'scroll_id': _scroll_id
    })
        
    scroll_res = requests.request(
    "POST", scroll_api_url,
    data=scroll_payload,
    headers=headers
    )
    
    #normalize data from raw json to columns
    result_dict_1 = json_normalize(data)
    
    #reform a specific dataset 
    dataset = DataFrame(result_dict_1,columns=['_index','_id','_type','_source.@timestamp','_source.CellID','_source.callin','_source.callout',
                                          '_source.countrycode','_source.datetime','_source.smsin','_source.smsout',
                                           '_source.internet'])
    
    #rename columns
    dataset.columns=['_index','_id','_type','timestamp','CellID','callin','callout','countrycode','datetime',
                     'smsin','smsout','internet']
    
    #print(dataset)
    
    #connect to sqlite database
    connex = db.connect("store_4.db") 
    cur = connex.cursor() 
    
    #export dataset to sqlite
    dataset.to_sql(name="cdr_dataset", con=connex, if_exists="append", index=True)   

    #scroll data with unique id (avoid duplicates)           
    try:
        res_json = scroll_res.json()
        data = res_json['hits']['hits']
        _scroll_id = res_json['_scroll_id']

    except KeyError:
        data = []
        _scroll_id = None
        err_msg = 'Error: Elastic Search Scroll: %s'
        print (err_msg % str(scroll_res.json()))


# In[37]:


connex = db.connect("store_4.db") 
cur = connex.cursor()


# In[44]:


#query SQL selections

#query = "SELECT * FROM cdr_dataset;"
#query = "SELECT * FROM cdr_dataset WHERE CellID=3387 AND internet>=100;"
#query = "SELECT * FROM cdr_dataset WHERE countrycode=39 AND internet>=10;"
#query = "SELECT datetime, CellID, internet FROM cdr_dataset WHERE internet>=100;"
query = "SELECT CellID, callin, callout, countrycode, datetime, smsin, smsout, internet FROM cdr_dataset;"


# In[45]:


df = pd.read_sql_query(query,connex)
print(df)


# In[47]:


# fill with 0 where is NaN
pandas_df = df.fillna(0)
pandas_df


# In[48]:


# see the columns of our dataset
pandas_df.columns


# In[50]:


#select columns from pandas
countrycode = DataFrame(pandas_df,columns=['countrycode'])
#countrycode

callin =  DataFrame(pandas_df,columns=['callin'])
#callin


# In[51]:


# making aggregations and conditions in pandas
pandas_df['callout_pricing'] = pandas_df.apply (lambda x: x['callout']*0.25 if x['countrycode']==221 else x['callout']*0.18, axis=1)
pandas_df['callin_pricing'] = pandas_df.apply (lambda x: x['callin']*0.019 if x['countrycode']==221 else x['callin']*0.011, axis=1) 
pandas_df['smsin_pricing'] = pandas_df.apply (lambda x: x['smsin']*0.01 if x['countrycode']==221 else x['smsin']*0.01, axis=1) 
pandas_df['smsout_pricing'] = pandas_df.apply (lambda x: x['smsout']*0.15 if x['countrycode']==221 else x['smsout']*0.1, axis=1) 
pandas_df['internet_pricing'] = pandas_df.apply (lambda x: x['internet']*0.7 if x['countrycode']==221 else x['internet']*0.4, axis=1) 


# In[53]:


print(pandas_df)


# In[54]:


# summurize all the column values in one total column
pandas_df['total_pricing'] = pandas_df.apply (lambda x: x['callout_pricing'] + x['callin_pricing'] + x['smsin_pricing'] + x['smsout_pricing'] + x['internet_pricing'] , axis=1)


# In[55]:


print(pandas_df)


# In[68]:


# add one more column in the dataset (index column in this case)
pandas_df['uid_name'] = (pandas_df.index + 0).astype(str)
df=pandas_df
print(df)


# In[69]:


# Export the final datast in csv file
# Don't forget to add '.csv' at the end of the path
export_csv = pandas_df.to_csv("C:/Users/LeliopoulosP/Desktop/ElasticPython/pricing_2.csv", index = None, header=True)


# In[74]:


# Convert a panda's dataframe to json
df = pd.read_csv("pricing_2.csv",sep=",",decimal=".")


# In[75]:


# Add a id for looping into elastic search index
df["no_index"] = [x+1 for x in range(len(df["datetime"]))]


# In[76]:


# Convert into json
tmp = df.to_json(orient = "records")


# In[77]:


# Load each record into json format before bulk
df_json= json.loads(tmp)
df_json[:10]


# In[78]:


# creating an index in the Elasticsearch
INDEX = 'cdr_pricing_1'
TYPE = 'bar_type'
esp = Espandas()
esp.es_write(df, INDEX, TYPE)


# In[79]:


# parce the dataset in the elasticsearch
def rec_to_actions(df):
    import json
    for record in df.to_dict(orient="records"):
        yield ('{ "index" : { "_index" : "%s", "_type" : "%s" }}'% (INDEX, TYPE))
        yield (json.dumps(record, default=int))


# In[80]:


e = Elasticsearch() # no args, connect to localhost:9200
if not e.indices.exists(INDEX):
    raise RuntimeError('index does not exists, use `curl -X PUT "localhost:9200/%s"` and try again'%INDEX)


# In[81]:


r = e.bulk(rec_to_actions(df)) # return a dict
print(not r["errors"])


# In[ ]:




