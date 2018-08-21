import zmq
import sys
import time
import _thread
import threading
import struct
from random import randint
import ECSCodes
import configparser
from Statemachine import Statemachine
import json
from DataObjects import partitionDataObject, detectorDataObject
import ECS_tools
import subprocess

class DetectorController:

    def __init__(self,id,startState="Shutdown"):
        self.context = zmq.Context()
        self.MyId = id
        self.currentTransitionNumber = None
        self.scriptProcess = None
        self.abort = False
        print(self.MyId)
        #get pca data from ECS
        config = configparser.ConfigParser()
        config.read("init.cfg")
        conf = config["Default"]
        self.receive_timeout = int(conf['receive_timeout'])

        #get Detector Information
        detectorData = None
        while detectorData == None:
            requestSocket = self.context.socket(zmq.REQ)
            requestSocket.connect("tcp://%s:%s" % (conf['ECSAddress'],conf['ECSRequestPort']))
            requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
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
            requestSocket = self.context.socket(zmq.REQ)
            requestSocket.connect("tcp://%s:%s" % (conf['ECSAddress'],conf['ECSRequestPort']))
            requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
            requestSocket.setsockopt(zmq.LINGER,0)

            requestSocket.send_multipart([ECSCodes.detectorAsksForPCA, self.MyId.encode()])
            try:
                pcaDataJSON = requestSocket.recv()
                if pcaDataJSON == ECSCodes.idUnknown:
                    print("I have not been assigned to any partition :(")
                    sys.exit(1)
                pcaDataJSON = json.loads(pcaDataJSON.decode())
                pcaData = partitionDataObject(pcaDataJSON)
            except zmq.Again:
                print("timeout getting pca Data")
                requestSocket.close()
                continue
            requestSocket.close()

        self.stateMap = ECS_tools.MapWrapper()

        confSection = ECS_tools.getConfsectionForType(detectorData.type)
        configDet = configparser.ConfigParser()
        configDet.read("detector.cfg")
        configDet = configDet[confSection]
        self.stateMachine = Statemachine(configDet["stateFile"],startState)

        self.pingSocket = self.context.socket(zmq.REP)
        self.pingSocket.bind(("tcp://*:%s" % self.pingPort))

        self.socketReceiver = self.context.socket(zmq.REP)
        self.socketReceiver.bind("tcp://*:%s" % self.port)

        self.socketSubscription = self.context.socket(zmq.SUB)
        self.socketSubscription.connect("tcp://%s:%s" % (pcaData.address,pcaData.portPublish))
        #subscribe to everything
        self.socketSubscription.setsockopt(zmq.SUBSCRIBE, b'')

        self.socketPushUpdate = self.context.socket(zmq.PUSH)
        self.socketPushUpdate.connect("tcp://%s:%s" % (pcaData.address,pcaData.portUpdates))
        #send state to PCA
        self.socketPushUpdate.send_multipart([self.MyId.encode(),ECS_tools.intToBytes(0),self.stateMachine.currentState.encode()])

        self.socketGetCurrentStateTable = self.context.socket(zmq.DEALER)
        self.socketGetCurrentStateTable.connect("tcp://%s:%s" % (pcaData.address,pcaData.portCurrentState))

        self.workThread = threading.Thread(name="worker", target=self.work)
        self.inTransition = False

        _thread.start_new_thread(self.waitForUpdates,())
        ECS_tools.getStateSnapshot(self.stateMap,pcaData.address,pcaData.portCurrentState,timeout=self.receive_timeout)
        _thread.start_new_thread(self.waitForPings,())
        _thread.start_new_thread(self.waitForCommand,())

    def changePCA(self,partition):
        """changes the current PCA"""
        self.socketSubscription.close()
        self.stateMap.reset()
        self.socketSubscription = self.context.socket(zmq.SUB)
        self.socketSubscription.connect("tcp://%s:%s" % (partition.address,partition.portPublish))
        #subscribe to everything
        self.socketSubscription.setsockopt(zmq.SUBSCRIBE, b'')

        self.socketPushUpdate.close()
        self.socketPushUpdate = self.context.socket(zmq.PUSH)
        self.socketPushUpdate.connect("tcp://%s:%s" % (partition.address,partition.portUpdates))
        #send state to PCA
        self.socketPushUpdate.send_multipart([self.MyId.encode(),ECS_tools.intToBytes(self.currentTransitionNumber),self.stateMachine.currentState.encode()])

        self.socketGetCurrentStateTable.close()
        self.socketGetCurrentStateTable = self.context.socket(zmq.DEALER)
        self.socketGetCurrentStateTable.connect("tcp://%s:%s" % (partition.address,partition.portCurrentState))

        ECS_tools.getStateSnapshot(self.stateMap,partition.address,partition.portCurrentState,timeout=self.receive_timeout)


    def waitForUpdates(self):
        #watch subscription for further updates
        while True:
            m = self.socketSubscription.recv_multipart()
            if len(m) != 3:
                print (m)
            else:
                id = m[0]
                sequence = m[1]
                state = m[2].decode()
            if state == ECSCodes.reset:
                self.stateMap.reset()
                print("reset")
                continue
            elif state == ECSCodes.removed:
                del self.stateMap[id]
                continue
            sequence = ECS_tools.intFromBytes(sequence)
            print("received update",id, sequence, state)
            self.stateMap[id] = (sequence, state)

    def work(self,command):
        self.scriptProcess = subprocess.Popen(["exec sh detectorScript.sh"], shell=True)
        self.scriptProcess.wait()
        if self.abort:
            self.inTransition = False
            self.abort = False
            return
        self.stateMachine.transition(command)
        self.socketPushUpdate.send_multipart([self.MyId.encode(),ECS_tools.intToBytes(self.currentTransitionNumber),self.stateMachine.currentState.encode()])
        self.inTransition = False

    def waitForCommand(self):
        while True:
            transitionNumber,command = self.socketReceiver.recv_multipart()
            command = command.decode()
            print (command,transitionNumber)
            if self.inTransition:
                self.socketReceiver.send(ECSCodes.busy)
                continue
            elif not self.stateMachine.checkIfPossible(command):
                self.socketReceiver.send(ECSCodes.error)
                continue
            else:
                self.socketReceiver.send(ECSCodes.ok)
            self.currentTransitionNumber = ECS_tools.intFromBytes(transitionNumber)
            print(self.currentTransitionNumber)
            self.inTransition = True
            self.workThread = threading.Thread(name="worker", target=self.work, args=(command,))
            self.workThread.start()

    def waitForPings(self):
        while True:
            command = self.pingSocket.recv_multipart()
            arg = None
            if len(command) > 1:
                arg = command[1].decode()
            command = command[0]
            if command == ECSCodes.ping:
                self.pingSocket.send(ECSCodes.ok)
                continue
            if command == ECSCodes.abort:
                #terminate Transition if active
                if self.inTransition:
                    print("abort")
                    self.abort = True
                    self.scriptProcess.terminate()
                self.pingSocket.send(ECSCodes.ok)
                continue
            if command == ECSCodes.pcaAsksForDetectorStatus:
                self.pingSocket.send(self.stateMachine.currentState.encode())
                continue
            if command == ECSCodes.detectorChangePartition:
                partition = partitionDataObject(json.loads(arg))
                self.changePCA(partition)
                self.pingSocket.send(ECSCodes.ok)
                continue
            self.pingSocket.send(ECSCodes.unknownCommand)



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
