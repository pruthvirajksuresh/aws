# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
#creating a spark session
from pyspark import SparkConf
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").config(conf=SparkConf()).getOrCreate()
df=spark.read.format("parquet").load("file:///D://pysparkdata/retail_db_parquet/orders")
df.show()

import json
i=1
while i<10:
    m={"first name":"name"+str(i)}
    print(json.dumps(m))
    kinesis.put_record("BotoDemo", json.dumps(m), "partitionkey")
    i=i+1
    
import boto3
client = boto3.resource(
    's3',
    aws_access_key_id="AKIAIKATXRWDDLV6BGQA",
    aws_secret_access_key="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"
)
for bucket in client.buckets.all():
    print(bucket.name)
    


sqs = boto3.resource('sqs',aws_access_key_id="AKIAIKATXRWDDLV6BGQA",
    aws_secret_access_key="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",region_name="us-east-1")

# Create the queue. This returns an SQS.Queue instance
queue = sqs.create_queue(QueueName='test', Attributes={'DelaySeconds': '5'})

# You can now access identifiers and attributes
print(queue.url)
print(queue.attributes.get('DelaySeconds'))


for queue in sqs.queues.all():
    print(queue.url)
    
    
queue = sqs.get_queue_by_name(QueueName='test')

queue.send_message(MessageBody='boto3', MessageAttributes={
    'Author': {
        'StringValue': 'Daniel',
        'DataType': 'String'
    }
})

for message in queue.receive_messages(MessageAttributeNames=['Author']):
    # Get the custom author message attribute if it was set
    author_text = ''
    if message.message_attributes is not None:
        author_name = message.message_attributes.get('Author').get('StringValue')
        if author_name:
            author_text = ' ({0})'.format(author_name)

    # Print out the body and author (if set)
    print('Hello, {0}!{1}'.format(message.body, author_text))














