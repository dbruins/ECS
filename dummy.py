#!/usr/bin/python3
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
from DataObjects import partitionDataObject, detectorDataObject, stateObject, transitionObject
import ECS_tools
import subprocess
import zc.lockfile #pip3 install zc.lockfile

class DetectorController:

    def __init__(self,id,startState="Shutdown"):
        #create lock
        try:
            self.lock = zc.lockfile.LockFile('/tmp/lock'+id, content_template='{pid}')
        except zc.lockfile.LockError:
            print("other Process is already Running "+id)
            exit(1)

        self.context = zmq.Context()
        self.MyId = id
        self.currentTransitionNumber = 0
        self.scriptProcess = None
        self.abort = False
        print(self.MyId)
        #get pca data from ECS
        config = configparser.ConfigParser()
        config.read("init.cfg")
        self.conf = config["Default"]
        self.receive_timeout = int(self.conf['receive_timeout'])

        #get Detector Information
        detectorData = None
        while detectorData == None:
            try:
                requestSocket = self.context.socket(zmq.REQ)
                requestSocket.connect("tcp://%s:%s" % (self.conf['ECSAddress'],self.conf['ECSRequestPort']))
                requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
                requestSocket.setsockopt(zmq.LINGER,0)

                requestSocket.send_multipart([ECSCodes.getDetectorForId, self.MyId.encode()])
                detectorDataJSON = requestSocket.recv()
                if detectorDataJSON == ECSCodes.idUnknown:
                    print("The ECS doesn't know who I am :(")
                    sys.exit(1)
                detectorDataJSON = json.loads(detectorDataJSON.decode())
                detectorData = detectorDataObject(detectorDataJSON)
            except zmq.Again:
                print("timeout getting detector Data")
                continue
            except zmq.error.ContextTerminated:
                pass
            finally:
                requestSocket.close()

        self.portTransition = detectorData.portTransition
        self.portCommand = detectorData.portCommand

        #get PCA Information
        pcaData = None
        while pcaData == None:
            pcaData = self.getPCAData()
        self.stateMap = ECS_tools.MapWrapper()

        confSection = ECS_tools.getConfsectionForType(detectorData.type)
        configDet = configparser.ConfigParser()
        configDet.read("detector.cfg")
        configDet = configDet[confSection]
        self.stateMachine = Statemachine(configDet["stateFile"],startState)
        self.pcaAddress = pcaData.address
        self.pcaUpdatePort = pcaData.portUpdates
        self.pcaID = pcaData.id

        self.commandSocket = self.context.socket(zmq.REP)
        self.commandSocket.bind("tcp://*:%s" % self.portCommand)

        self.socketReceiver = self.context.socket(zmq.REP)
        self.socketReceiver.bind("tcp://*:%s" % self.portTransition)

        #Subscription needs its own context so we can terminate it seperately in case of a pca Change
        self.subContext = zmq.Context()
        self.socketSubscription = self.subContext.socket(zmq.SUB)
        self.socketSubscription.connect("tcp://%s:%s" % (pcaData.address,pcaData.portPublish))
        #subscribe to everything
        self.socketSubscription.setsockopt(zmq.SUBSCRIBE, b'')

        #send state to PCA
        self.sendUpdate()

        self.workThread = threading.Thread(name="worker", target=self.work)
        self.inTransition = False
        self.currentCommand = ""

        _thread.start_new_thread(self.waitForUpdates,())
        ECS_tools.getStateSnapshot(self.stateMap,pcaData.address,pcaData.portCurrentState,timeout=self.receive_timeout)
        _thread.start_new_thread(self.waitForCommands,())
        _thread.start_new_thread(self.waitForTransition,())

    def getPCAData(self):
        """get PCA Information from ECS"""
        requestSocket = self.context.socket(zmq.REQ)
        requestSocket.connect("tcp://%s:%s" % (self.conf['ECSAddress'],self.conf['ECSRequestPort']))
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
            return None
        except zmq.error.ContextTerminated:
            requestSocket.close()
        finally:
            requestSocket.close()
        return pcaData

    def changePCA(self,partition):
        """changes the current PCA to the one specified in the ECS database"""
        self.stateMap.reset()
        self.pcaAddress = partition.address
        self.pcaUpdatePort = partition.portUpdates
        self.pcaID = partition.id

        #blocks until subscription socket is closed
        self.subContext.term()
        self.subContext =  zmq.Context()
        self.socketSubscription = self.subContext.socket(zmq.SUB)
        self.socketSubscription.connect("tcp://%s:%s" % (partition.address,partition.portPublish))
        #subscribe to everything
        self.socketSubscription.setsockopt(zmq.SUBSCRIBE, b'')
        _thread.start_new_thread(self.waitForUpdates,())

        ECS_tools.getStateSnapshot(self.stateMap,partition.address,partition.portCurrentState,timeout=self.receive_timeout)


    def sendUpdate(self):
        """send current state to PCA"""
        try:
            socketSendUpdateToPCA = self.context.socket(zmq.REQ)
            socketSendUpdateToPCA.connect("tcp://%s:%s" % (self.pcaAddress,self.pcaUpdatePort))
            socketSendUpdateToPCA.send_multipart([self.MyId.encode(),ECS_tools.intToBytes(self.currentTransitionNumber),self.stateMachine.currentState.encode()])
            r = socketSendUpdateToPCA.recv()
            if r == ECSCodes.idUnknown:
                socketSendUpdateToPCA.close()
                data = self.getPCAData()
                self.changePCA(data)
        except zmq.Again:
            print("timeout sending status")
        except zmq.error.ContextTerminated:
            pass
        except Exception as e:
            print("error sending status: %s" % str(e))
        finally:
            socketSendUpdateToPCA.close()


    def waitForUpdates(self):
        #watch subscription for further updates
        while True:
            try:
                m = self.socketSubscription.recv_multipart()
            except zmq.error.ContextTerminated:
                self.socketSubscription.close()
                break

            if len(m) != 3:
                print (m)
            else:
                id = m[0]
                sequence = m[1]
                state = m[2]
            if state == ECSCodes.reset:
                self.stateMap.reset()
                print("reset")
                continue
            elif state == ECSCodes.removed:
                del self.stateMap[id]
                continue
            state = json.loads(state.decode())
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
        self.sendUpdate()
        self.inTransition = False

    def waitForTransition(self):
        while True:
            try:
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
                self.currentCommand = command
                self.inTransition = True
                self.workThread = threading.Thread(name="worker", target=self.work, args=(command,))
                self.workThread.start()
            except zmq.error.ContextTerminated:
                self.socketReceiver.close()
                break

    def waitForCommands(self):
        while True:
            try:
                command = self.commandSocket.recv_multipart()
                arg = None
                if len(command) > 1:
                    arg = command[1].decode()
                command = command[0]
                if command == ECSCodes.ping:
                    self.commandSocket.send(ECSCodes.ok)
                    continue
                if command == ECSCodes.abort:
                    #terminate Transition if active
                    if self.inTransition:
                        print("abort")
                        self.abort = True
                        self.scriptProcess.terminate()
                    self.commandSocket.send(ECSCodes.ok)
                    continue
                if command == ECSCodes.pcaAsksForDetectorStatus:
                    if not self.inTransition:
                        transition = None
                    else:
                        transition = transitionObject([self.currentTransitionNumber,self.currentCommand,self.stateMachine.getNextStateForCommand(self.currentCommand)])
                    state = stateObject([self.stateMachine.currentState,transition])
                    self.commandSocket.send(state.asJsonString().encode())
                    continue
                if command == ECSCodes.detectorChangePartition:
                    partition = partitionDataObject(json.loads(arg))
                    self.changePCA(partition)
                    self.commandSocket.send(ECSCodes.ok)
                    continue
                if command == ECSCodes.check:
                    self.commandSocket.send(ECSCodes.ok)
                    partition = partitionDataObject(json.loads(arg))
                    if partition.id != self.pcaID:
                        self.changePCA(partition)
                    continue
                self.commandSocket.send(ECSCodes.unknownCommand)
            except zmq.error.ContextTerminated:
                self.commandSocket.close()
                break

    def terminate(self):
        self.context.term()
        self.subContext.term()
        self.abort = True
        if self.scriptProcess:
            self.scriptProcess.terminate()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("please enter the detector id")
        sys.exit(1)
    if len(sys.argv) == 3:
        test = DetectorController(sys.argv[1],sys.argv[2])
    else:
        test = DetectorController(sys.argv[1])
    while True:
        try:
            x = input()
        except KeyboardInterrupt:
            test.terminate()
            break
        except EOFError:
            time.sleep(500000)
            continue
