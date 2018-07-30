import zmq
import sys
import time
import _thread
import struct
from random import randint
import ECSCodes
import configparser
from Statemachine import Statemachine
import json
from DataObjects import partitionDataObject, detectorDataObject
from ECS_tools import getConfsectionForType

class DetectorController:

    def __init__(self,id,startState="Shutdown"):
        context = zmq.Context()
        self.MyId = id
        print(self.MyId)
        #get pca data from ECS
        config = configparser.ConfigParser()
        config.read("init.cfg")
        conf = config["Default"]

        #get Detector Information
        detectorData = None
        while detectorData == None:
            requestSocket = context.socket(zmq.REQ)
            requestSocket.connect("tcp://%s:%s" % (conf['ECSAddress'],conf['ECSRequestPort']))
            requestSocket.setsockopt(zmq.RCVTIMEO, int(conf['receive_timeout']))
            requestSocket.setsockopt(zmq.LINGER,0)

            requestSocket.send_multipart([ECSCodes.getDetectorForId, self.MyId.encode()])
            try:
                detectorDataJSON = requestSocket.recv()
                if detectorDataJSON == ECSCodes.idUnknown:
                    print("The ECS doesn't know who I am :(")
                    sys.exit(1)
                detectorDataJSON = json.loads(detectorDataJSON.decode())
                detectorData = detectorDataObject(detectorDataJSON)
            except zmq.Again:
                print("timeout getting detector Data")
                requestSocket.close()
                continue
            requestSocket.close()

        self.port = detectorData.port
        self.pingPort = detectorData.pingPort

        #get PCA Information
        pcaData = None
        while pcaData == None:
            requestSocket = context.socket(zmq.REQ)
            requestSocket.connect("tcp://%s:%s" % (conf['ECSAddress'],conf['ECSRequestPort']))
            requestSocket.setsockopt(zmq.RCVTIMEO, int(conf['receive_timeout']))
            requestSocket.setsockopt(zmq.LINGER,0)

            requestSocket.send_multipart([ECSCodes.detectorAsksForPCA, self.MyId.encode()])
            try:
                pcaDataJSON = requestSocket.recv()
                if pcaDataJSON == ECSCodes.idUnknown:
                    print("I have not assigned to any partition :(")
                    sys.exit(1)
                pcaDataJSON = json.loads(pcaDataJSON.decode())
                pcaData = partitionDataObject(pcaDataJSON)
            except zmq.Again:
                print("timeout getting pca Data")
                requestSocket.close()
                continue
            requestSocket.close()

        self.stateMap = {}

        confSection = getConfsectionForType(detectorData.type)
        configDet = configparser.ConfigParser()
        configDet.read("detector.cfg")
        configDet = configDet[confSection]
        self.stateMachine = Statemachine(configDet["stateFile"],startState)

        self.pingSocket = context.socket(zmq.REP)
        self.pingSocket.bind(("tcp://*:%s" % self.pingPort))

        self.socketReceiver = context.socket(zmq.PAIR)
        self.socketReceiver.bind("tcp://*:%s" % self.port)

        self.socketSubscription = context.socket(zmq.SUB)
        self.socketSubscription.connect("tcp://%s:%s" % (pcaData.address,pcaData.portPublish))
        #subscribe to everything
        self.socketSubscription.setsockopt(zmq.SUBSCRIBE, b'')

        self.socketPushUpdate = context.socket(zmq.PUSH)
        self.socketPushUpdate.connect("tcp://%s:%s" % (pcaData.address,pcaData.portUpdates))
        self.socketPushUpdate.send_string("%s %s" % (self.MyId, startState))


        self.socketGetCurrentStateTable = context.socket(zmq.DEALER)
        self.socketGetCurrentStateTable.connect("tcp://%s:%s" % (pcaData.address,pcaData.portCurrentState))

        _thread.start_new_thread(self.waitForCommand,())
        _thread.start_new_thread(self.waitForUpdates,())
        _thread.start_new_thread(self.waitForPings,())

    def receive_status(self,socket):
        try:
            id, sequence, state = socket.recv_multipart()
        except:
            print ("receive error")
            return
        if id != b"":
            id = id.decode()
        else:
            id = None
        sequence = struct.unpack("!i",sequence)[0]
        if state != b"":
            state = state.decode()
        else:
            state = None
        return [id,sequence,state]

    def waitForUpdates(self):
        #get current state
        # Get state snapshot
        sequence = 0
        self.socketGetCurrentStateTable.send(ECSCodes.hello)
        while True:
            id, sequence, state = self.receive_status(self.socketGetCurrentStateTable)
            print (id,sequence,state)
            if id != None:
                self.stateMap[id] = (sequence, state)
            #id should be None in final message
            else:
                break

        #watch subscription for further updates
        while True:
            m = self.socketSubscription.recv_multipart()
            if len(m) != 3:
                print (m)
            else:
                id = m[0]
                sequence = m[1]
                state = m[2].decode()
            id = id.decode()

            sequence = struct.unpack("!i",sequence)[0]
            print("received update",id, sequence, state)
            self.stateMap[id] = (sequence, state)

    def waitForCommand(self):
        while True:
            m = self.socketReceiver.recv()
            if m == ECSCodes.pcaAsksForDetectorStatus:
                self.socketReceiver.send(self.stateMachine.currentState.encode())
                continue
            self.socketReceiver.send(ECSCodes.ok)
            if(m != ECSCodes.ping):
                print (m)
            if self.stateMachine.transition(m.decode()):
                time.sleep(randint(2,6))
                self.socketReceiver.send(self.stateMachine.currentState.encode())
            else:
                self.socketReceiver.send(ECSCodes.error)

    def waitForPings(self):
        while True:
            self.pingSocket.recv()
            self.pingSocket.send(ECSCodes.ok)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("please enter the detector id")
        sys.exit(1)
    if len(sys.argv) == 3:
        test = DetectorController(sys.argv[1],sys.argv[2])
    else:
        test = DetectorController(sys.argv[1])
    while True:
        if len(sys.argv) > 2:
            continue
        try:
            x = input()
        except EOFError:
            continue
        test.socketPushUpdate.send_string("%s %s" % (test.MyId, x))
