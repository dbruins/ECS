#!/usr/bin/python3
import zmq
import sys
import time
import _thread
import threading
from ECSCodes import ECSCodes
codes = ECSCodes()
import configparser
from Statemachine import Statemachine
import json
from DataObjects import partitionDataObject, detectorDataObject, stateObject, configObject
from states import DetectorStates, DetectorTransitions
import ECS_tools
import subprocess
import zc.lockfile
from multiprocessing import Queue
import os
import errno

class DetectorController:

    def __init__(self,detectorData,startState="Unconfigured"):
        self.MyId = detectorData.id
        #create lock
        try:
            self.lock = zc.lockfile.LockFile('/tmp/lock'+self.MyId, content_template='{pid}')
        except zc.lockfile.LockError:
            print("other Process is already Running "+self.MyId)
            exit(1)

        self.scriptProcess = None
        self.abort = False
        #Lock to handle StateMachine Transition access
        self.stateMachineLock = threading.Lock()

        config = configparser.ConfigParser()
        config.read("init.cfg")
        self.configFile = config["Default"]

        self.portTransition = detectorData.portTransition
        self.portCommand = detectorData.portCommand
        #seuquence Number for sending Updates
        self.sequenceNumber = 0
        self.receive_timeout = int(self.configFile['receive_timeout'])

        self.context = zmq.Context()
        self.context.setsockopt(zmq.LINGER,0)

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
        self.configTag = None
        self.config = None
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
        t = threading.Thread(name='update'+str(0), target=self.sendUpdate, args=(0,))
        t.start()

        self.inTransition = False

        _thread.start_new_thread(self.waitForUpdates,())
        ECS_tools.getStateSnapshot(self.stateMap,pcaData.address,pcaData.portCurrentState,timeout=self.receive_timeout)
        _thread.start_new_thread(self.waitForCommands,())
        _thread.start_new_thread(self.waitForTransition,())
        _thread.start_new_thread(self.waitForPipeMessages,())

    def waitForPipeMessages(self):
        try:
            os.mkfifo("pipeDetector"+self.MyId)
        except OSError as oe:
            #Ignore file already exists Exception
            if oe.errno != errno.EEXIST:
                #re-raise all other Exceptions
                raise
        while True:
            with open("pipeDetector"+self.MyId) as fifo:
                while True:
                    message = fifo.read().replace('\n', '')
                    if len(message) == 0:
                        #pipe closed
                        break
                    if message == "error":
                        self.error()
                    elif message == "resolved":
                        self.resolved()
                    else:
                        print('received unknown message via pipe: %s' % (message,))

    def getPCAData(self):
        """get PCA Information from ECS"""
        requestSocket = self.context.socket(zmq.REQ)
        requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        requestSocket.connect("tcp://%s:%s" % (self.configFile['ECSAddress'],self.configFile['ECSRequestPort']))

        requestSocket.send_multipart([codes.detectorAsksForPCA, self.MyId.encode()])
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


    def sendUpdate(self,sequenceNumber,comment=None):
        """send current state to PCA"""
        data = {
            "id": self.MyId,
            "state": self.stateMachine.currentState,
            "sequenceNumber": sequenceNumber,
        }
        if self.configTag:
            data["tag"] = self.configTag
        if comment:
            data["comment"] = comment
        #repeat until update is received
        while True:
            try:
                print(data)
                socketSendUpdateToPCA = self.context.socket(zmq.REQ)
                socketSendUpdateToPCA.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
                socketSendUpdateToPCA.connect("tcp://%s:%s" % (self.pcaAddress,self.pcaUpdatePort))
                socketSendUpdateToPCA.send(json.dumps(data).encode())
                #try to receive Ok from PCA
                r = socketSendUpdateToPCA.recv()
                if r == codes.idUnknown:
                    print("wrong PCA")
                    socketSendUpdateToPCA.close()
                    dataPCA = self.getPCAData()
                    self.changePCA(dataPCA)
                else:
                    #success
                    return
            except zmq.Again:
                print("timeout sending status")
                if sequenceNumber < self.sequenceNumber:
                    #if there is already a new update give up
                    return
            except zmq.error.ContextTerminated:
                #Agent is being terminated
                return
            except Exception as e:
                print("error sending status: %s" % str(e))
                raise Exception("error sending status")
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
            if state == codes.reset:
                self.stateMap.reset()
                print("reset")
                continue
            elif state == codes.removed:
                del self.stateMap[id]
                continue
            state = json.loads(state.decode())
            sequence = ECS_tools.intFromBytes(sequence)
            #print("received update",id, sequence, state)
            self.stateMap[id] = (sequence, state)

    def waitForTransition(self):
        while True:
            try:
                ret = self.socketReceiver.recv_multipart()
                print(ret)
                if len(ret)  == 2:
                    command,conf = ret
                    command = command.decode()
                    conf =  configObject(json.loads(conf.decode()))
                else:
                    command = ret[0].decode()
                    conf=None
                if self.stateMachine.currentState not in {DetectorStates.Active,DetectorStates.Unconfigured}  and command not in {DetectorTransitions.abort,DetectorTransitions.reset}:
                    self.socketReceiver.send(codes.busy)
                    print("busy")
                    continue
                elif not self.stateMachine.checkIfPossible(command):
                    self.socketReceiver.send(codes.error)
                    print("error")
                    continue
                else:
                    self.socketReceiver.send(codes.ok)
                self.abort = False
                if command == DetectorTransitions.configure:
                    self.transition(DetectorTransitions.configure,conf)
                    self.inTransition = True
                    workThread = threading.Thread(name="worker", target=self.configure, args=(conf,))
                    workThread.start()
                elif command == DetectorTransitions.abort:
                    self.abortFunction()
                elif command == DetectorTransitions.reset:
                    self.reset()
                elif command == DetectorTransitions.error:
                    self.error()
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
                if command == codes.ping:
                    self.commandSocket.send(codes.ok)
                    continue
                if command == codes.pcaAsksForDetectorStatus:
                    if self.configTag:
                        self.commandSocket.send_multipart([self.stateMachine.currentState.encode(),self.configTag.encode()])
                    else:
                        self.commandSocket.send_multipart([self.stateMachine.currentState.encode()])
                    continue
                if command == codes.detectorChangePartition:
                    partition = partitionDataObject(json.loads(arg))
                    self.changePCA(partition)
                    self.commandSocket.send(codes.ok)
                    continue
                if command == codes.check:
                    self.commandSocket.send(codes.ok)
                    partition = partitionDataObject(json.loads(arg))
                    if partition.id != self.pcaID:
                        self.changePCA(partition)
                    continue
                self.commandSocket.send(codes.unknownCommand)
            except zmq.error.ContextTerminated:
                self.commandSocket.close()
                break

    def terminate(self):
        self.context.term()
        self.subContext.term()
        self.abort = True
        if self.scriptProcess:
            self.scriptProcess.terminate()

    def configure(self):
        pass

    def abortFunction(self):
        pass

    def error(self):
        self.transition(DetectorTransitions.error)
        if self.inTransition:
            self.abort = True
            if self.scriptProcess:
                self.scriptProcess.terminate()

    def resolved(self):
        self.transition(DetectorTransitions.resolved)

    def reset(self):
        self.transition(DetectorTransitions.reset)

    def transition(self,transition,conf=None,comment=None):
        try:
            self.stateMachineLock.acquire()
            if self.stateMachine.transition(transition):
                if conf:
                    self.configTag = conf.configId
                else:
                    self.configTag = None
                self.config=conf
                self.sequenceNumber = self.sequenceNumber+1
                sequenceNumber = self.sequenceNumber
                self.stateMachineLock.release()
                t = threading.Thread(name='update'+str(sequenceNumber), target=self.sendUpdate, args=(sequenceNumber,comment))
                t.start()
        finally:
            if self.stateMachineLock.locked():
                self.stateMachineLock.release()

    def executeScript(self,scriptname):
        self.scriptProcess = subprocess.Popen(["exec ./"+scriptname], shell=True)
        ret = self.scriptProcess.wait()
        if ret:
            return False
        else:
            return True

class DetectorA(DetectorController):

    def configure(self,conf):
        if conf.configId != self.config.configId:
            self.abortFunction()
        for i in range(0,2):
            ret = self.executeScript("detectorScript.sh")
            if ret:
                self.transition(DetectorTransitions.success,conf)
            else:
                if self.abort:
                    self.abort = False
                    self.transition(DetectorTransitions.abort,comment="transition aborted")
                    self.inTransition = False
                    break
                self.transition(DetectorTransitions.error,comment="transition failed")
                self.inTransition = False
                break
        self.inTransition = False

    def abortFunction(self):
        #terminate Transition if active
        if self.inTransition:
            self.abort = True
            if self.scriptProcess:
                self.scriptProcess.terminate()
        else:
            self.transition(DetectorTransitions.abort)

class DetectorB(DetectorController):

    def configure(self,conf):
        for i in range(0,3):
            ret = self.executeScript("detectorScript.sh")
            if ret:
                self.transition(DetectorTransitions.success,conf)
            else:
                if self.abort:
                    self.abort = False
                    self.transition(DetectorTransitions.abort,comment="transition aborted")
                    self.inTransition = False
                    break
                self.transition(DetectorTransitions.error,comment="transition failed")
                self.inTransition = False
                break
        self.inTransition = False

    def abortFunction(self):
        #terminate Transition if active
        if self.inTransition:
            self.abort = True
            if self.scriptProcess:
                self.scriptProcess.terminate()
        else:
            self.transition(DetectorTransitions.abort)



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("please enter the detector id")
        sys.exit(1)
    else:
        #get Detector Information
        config = configparser.ConfigParser()
        config.read("init.cfg")
        conf = config["Default"]
        detectorData = None
        while detectorData == None:
            try:
                context = zmq.Context()
                requestSocket = context.socket(zmq.REQ)
                requestSocket.connect("tcp://%s:%s" % (conf['ECSAddress'],conf['ECSRequestPort']))
                requestSocket.setsockopt(zmq.RCVTIMEO, int(conf['receive_timeout']))
                requestSocket.setsockopt(zmq.LINGER,0)

                requestSocket.send_multipart([codes.getDetectorForId, sys.argv[1].encode()])
                detectorDataJSON = requestSocket.recv()
                if detectorDataJSON == codes.idUnknown:
                    print("The ECS doesn't know who I am :(")
                    sys.exit(1)
                detectorDataJSON = json.loads(detectorDataJSON.decode())
                detectorData = detectorDataObject(detectorDataJSON)
            except zmq.Again:
                print("timeout getting detector Data")
                continue
            finally:
                requestSocket.close()
                context.term()
        if detectorData.type == "DetectorA":
            test = DetectorA(detectorData)
        elif detectorData.type == "DetectorB":
            test = DetectorB(detectorData)
        else:
            raise Exception("Detector Type unknown")
    while True:
        try:
            x = input()
            if x == "error":
                test.error()
            if x == "resolved":
                test.resolved()
        except KeyboardInterrupt:
            test.terminate()
            break
        except EOFError:
            time.sleep(50000000)
            continue
