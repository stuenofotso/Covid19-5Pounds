from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np
from elasticsearch.helpers import scan,bulk
import uuid



df = pd.read_csv("../../covid19_world_forcasted_data.csv", sep=';')[['ds', 'confirmed', 'deaths', 'recovered', 'confirmed_yhat', 'confirmed_yhat_lower', 'confirmed_yhat_upper', 'deaths_yhat', 'deaths_yhat_lower', 'deaths_yhat_upper', 'recovered_yhat', 'recovered_yhat_lower', 'recovered_yhat_upper']]

df[['confirmed', 'deaths', 'recovered', 'confirmed_yhat', 'confirmed_yhat_lower', 'confirmed_yhat_upper', 'deaths_yhat', 'deaths_yhat_lower', 'deaths_yhat_upper', 'recovered_yhat', 'recovered_yhat_lower', 'recovered_yhat_upper']] \
    = df[['confirmed', 'deaths', 'recovered', 'confirmed_yhat', 'confirmed_yhat_lower', 'confirmed_yhat_upper', 'deaths_yhat', 'deaths_yhat_lower', 'deaths_yhat_upper', 'recovered_yhat', 'recovered_yhat_lower', 'recovered_yhat_upper']].astype("float32")


df['ds'] =  pd.to_datetime(df['ds'],
                             format='%Y-%m-%d')

df.fillna(-1.0, inplace=True)



def filterKeys(document):
    return {key: document[key] for key in df.columns }



es = Elasticsearch(["ec2-3-135-203-125.us-east-2.compute.amazonaws.com","ec2-13-58-53-1.us-east-2.compute.amazonaws.com","ec2-18-220-232-235.us-east-2.compute.amazonaws.com","ec2-18-224-63-68.us-east-2.compute.amazonaws.com"],
                   http_auth=('elastic', 'W|Hed%/E$]=('),
                   port=9200,
                   use_ssl=False
                   )


def doc_generator(df_):
    for index, document in df_.iterrows():
        yield {
            "_index": 'covid19_prediction_data',
            "_type": "_doc",
            "_id" : uuid.uuid1(),
            "_source": filterKeys(document),
        }
    raise StopIteration

print("bulk insert started...")

bulk(es, doc_generator(df))

print("bulk insert ended")


