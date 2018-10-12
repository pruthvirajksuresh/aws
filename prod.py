# -*- coding: utf-8 -*-
"""
Created on Fri Oct 12 12:55:46 2018

@author: pruthviraj.suresh
"""

import boto3
import json
import time
my_stream_name = 'python-stream'
kinesis_client = boto3.client('kinesis', region_name='us-east-1',
                              aws_access_key_id="AKIAIKATXRWDDLV6BGQA",
                              aws_secret_access_key="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")
i=1
while i<100:
    m={"first name":"name"+str(i)}
    print(json.dumps(m))
    response = kinesis_client.describe_stream(StreamName=my_stream_name)

    my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = kinesis_client.get_shard_iterator(StreamName=my_stream_name,
                                                      ShardId=my_shard_id,
                                                      ShardIteratorType='LATEST')
    print(kinesis_client.get_shard_iterator(StreamName=my_stream_name,
                                                      ShardId=my_shard_id,
                                                      ShardIteratorType='LATEST'))
    
    kinesis_client.put_record(StreamName=my_stream_name,Data=json.dumps(m),PartitionKey="123456789")

    time.sleep(7)
    i=i+1
