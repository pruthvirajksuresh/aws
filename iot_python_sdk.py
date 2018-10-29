
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import argparse
import json

host = "a13lbkikpf9twg-ats.iot.us-east-2.amazonaws.com"
rootCAPath = "root-CA.crt"
certificatePath = "aws_preethi.cert.pem"
privateKeyPath = "aws_preethi.private.key"


port = 8883
useWebsocket = False
clientId = "sdk-java"
topic = "topic_1"
message_to_print="aws aws_preethi"

myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId)
myAWSIoTMQTTClient.configureEndpoint(host, port)
myAWSIoTMQTTClient.configureCredentials(rootCAPath, privateKeyPath, certificatePath)

# AWSIoTMQTTClient connection configuration
myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
myAWSIoTMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
myAWSIoTMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
myAWSIoTMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec

# Connect and subscribe to AWS IoT
myAWSIoTMQTTClient.connect()

# Publish to the same topic in a loop forever
loopCount = 0
while True:
    message = {}
    message['message'] = message_to_print
    message['sequence'] = loopCount
    messageJson = json.dumps(message)
    myAWSIoTMQTTClient.publish(topic, messageJson, 1)
    print('Published topic %s: %s\n' % (topic,messageJson))
    loopCount += 1
    time.sleep(10)