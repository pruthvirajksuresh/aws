# -*- coding: utf-8 -*-
"""
Created on Fri Oct 12 12:55:58 2018

@author: pruthviraj.suresh
"""

import boto3
import time

my_stream_name = 'python-stream'

kinesis_client = boto3.client('kinesis', region_name='us-east-1',
                              aws_access_key_id="AKIAIKATXRWDDLV6BGQA",
                              aws_secret_access_key="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")

response = kinesis_client.describe_stream(StreamName=my_stream_name)

my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']

shard_iterator = kinesis_client.get_shard_iterator(StreamName=my_stream_name,
                                                      ShardId=my_shard_id,
                                                      ShardIteratorType='LATEST')

my_shard_iterator = shard_iterator['ShardIterator']
record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator,
                                              Limit=2)
while 'NextShardIterator' in record_response:
    record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'],
                                                  Limit=2)
    print(record_response['Records'][0]['Data'])
    time.sleep(7)