from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np
from elasticsearch.helpers import scan





es = Elasticsearch(["ec2-3-135-203-125.us-east-2.compute.amazonaws.com","ec2-13-58-53-1.us-east-2.compute.amazonaws.com","ec2-18-220-232-235.us-east-2.compute.amazonaws.com","ec2-18-224-63-68.us-east-2.compute.amazonaws.com"],
                   http_auth=('elastic', 'W|Hed%/E$]=('),
                   port=9200,
                   use_ssl=False
                   )
res_scan = scan(
    es,
    index="covid19_raw_data",
    query={
        "track_total_hits": True,
        'query': {'match_all': {}}
    }
)

raw_df = pd.DataFrame(d['_source'] for d in res_scan)

# df = pd.json_normalize(res['hits']['hits'])[['_source.Country','_source.Date','_source.Cases', '_source.Status']] \
#     .astype({'_source.Cases': 'float32'})


df = raw_df[['Country', 'CountryCode','Confirmed','Deaths', 'Recovered','Date']].astype({'Confirmed': 'float32'}).astype({'Deaths': 'float32'}).astype({'Recovered': 'float32'})


df['Date'] =  pd.to_datetime(df['Date'],
                              format='%d-%m-%Y %H:%M:%S')

df = df.groupby(['Country', 'CountryCode','Date']).agg({'Confirmed':sum, 'Deaths':sum, 'Recovered':sum})

print(df.columns)

print(df.head(10))

df.to_csv("../../covid19_grouped_data.csv", sep=';')