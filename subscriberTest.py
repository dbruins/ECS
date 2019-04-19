#!/usr/bin/python3
import zmq
import sys
import threading
from DataObjects import partitionDataObject
from ECSCodes import ECSCodes
codes = ECSCodes()
import configparser
import json

#create n subscribers for a pca

if len(sys.argv) < 3:
    print("please provide number of subscriptions and some detector Id")
    sys.exit(1)

n = int(sys.argv[1])
detId = sys.argv[2]

subContext = zmq.Context()

def getPCAData():
    """get PCA Information from ECS"""
    config = configparser.ConfigParser()
    config.read("init.cfg")
    configFile = config["Default"]

    requestSocket = subContext.socket(zmq.REQ)
    requestSocket.connect("tcp://%s:%s" % (configFile['ECAAddress'],configFile['ECARequestPort']))

    requestSocket.send_multipart([codes.detectorAsksForPCA, detId.encode()])
    try:
        pcaDataJSON = requestSocket.recv()
        if pcaDataJSON == codes.idUnknown:
            print("I have not been assigned to any partition :(")
            sys.exit(1)
        pcaDataJSON = json.loads(pcaDataJSON.decode())
        pcaData = partitionDataObject(pcaDataJSON)
    except zmq.Again:
        print("timeout getting pca Data")
        requestSocket.close()
        return None
    except zmq.error.ContextTerminated:
        requestSocket.close()
    finally:
        requestSocket.close()
    return pcaData
pcaData = getPCAData()
def subscribe():
    socketSubscription = subContext.socket(zmq.SUB)
    socketSubscription.connect("tcp://%s:%s" % (pcaData.address,pcaData.portPublish))
    #subscribe to everything
    socketSubscription.setsockopt(zmq.SUBSCRIBE, b'')
    while True:
        try:
            m = socketSubscription.recv_multipart()
            print(m)
        except zmq.error.ContextTerminated:
            socketSubscription.close()
            break

for i in range(n):
    t = threading.Thread(target=subscribe)
    t.start()

input()
subContext.term()
