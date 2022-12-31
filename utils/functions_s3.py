#!/usr/bin/env python
# coding: utf-8

# In[ ]:


"""
amazon s3 related functions

"""
import pandas as pd
import boto3
import json
from io import StringIO

def s3_pd_read_csv(bucket, key):
    s3 = boto3.client('s3')
    csvfile = s3.get_object(Bucket = bucket, Key = key)
    df = pd.read_csv(csvfile['Body'], sep = ',')
    return df
    
def s3_write_json(bucket, key, json_item):
    s3 = boto3.resource('s3')
    s3object = s3.Object(bucket, key)
    s3object.put(
        Body=(bytes(json.dumps(json_item).encode('UTF-8'))))


def s3_write_csv(bucket, key, df):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3 = boto3.resource('s3')
    s3object = s3.Object(bucket, key)
    s3object.put(
        Body=csv_buffer.getvalue())
    
# def get_object_s3(bucket, key):
#     csvfile = s3.get_object(Bucket = 'dataspartan-test-bucket', Key = 'twitter_key_pool_0.csv')
#     df = pd.read_csv(csvfile['Body'], sep = ',')
#     twitter_tokens = list(df['token'])

