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
from DataObjects import partitionDataObject, detectorDataObject, stateObject
from states import DetectorStates, DetectorTransitions
import ECS_tools
import subprocess
import zc.lockfile

class DetectorController:

    def __init__(self,detectorData,startState="Unconfigured"):
        self.MyId = detectorData.id
        #create lock
        try:
            self.lock = zc.lockfile.LockFile('/tmp/lock'+self.MyId, content_template='{pid}')
        except zc.lockfile.LockError:
            print("other Process is already Running "+self.MyId)
            exit(1)
        self.context = zmq.Context()
        self.scriptProcess = None
        self.abort = False

        config = configparser.ConfigParser()
        config.read("init.cfg")
        self.conf = config["Default"]

        self.portTransition = detectorData.portTransition
        self.portCommand = detectorData.portCommand
        self.receive_timeout = int(self.conf['receive_timeout'])

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

        self.inTransition = False

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


    def sendUpdate(self,comment=None):
        """send current state to PCA"""
        data = {
            "id": self.MyId,
            "state": self.stateMachine.currentState
        }
        if self.configTag:
            data["tag"] = self.configTag
        if comment:
            data["comment"] = comment
        try:
            socketSendUpdateToPCA = self.context.socket(zmq.REQ)
            socketSendUpdateToPCA.connect("tcp://%s:%s" % (self.pcaAddress,self.pcaUpdatePort))
            socketSendUpdateToPCA.send(json.dumps(data).encode())
            r = socketSendUpdateToPCA.recv()
            if r == codes.idUnknown:
                print("wrong PCA")
                socketSendUpdateToPCA.close()
                data = self.getPCAData()
                self.changePCA(data)
                self.sendUpdate(comment)
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
        pass

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

    def transition(self,transition,tag=None):
        self.stateMachine.transition(transition)
        self.configTag = tag

class DetectorA(DetectorController):

    def waitForTransition(self):
        while True:
            try:
                ret = self.socketReceiver.recv_multipart()
                print(ret)
                if len(ret)  == 2:
                    command,tag = ret
                    command = command.decode()
                    tag = tag.decode()
                else:
                    command = ret[0].decode()
                    tag=None
                if self.stateMachine.currentState not in {DetectorStates.Active,DetectorStates.Unconfigured} and command != DetectorTransitions.abort:
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
                    self.transition(DetectorTransitions.configure,tag)
                    self.inTransition = True
                    self.sendUpdate()
                    workThread = threading.Thread(name="worker", target=self.getReady, args=(tag,))
                    workThread.start()
                if command == DetectorTransitions.abort:
                    self.abortFunction()
            except zmq.error.ContextTerminated:
                self.socketReceiver.close()
                break

    def getReady(self,tag):
        for i in range(0,2):
            ret = self.executeScript("detectorScript.sh")
            if ret:
                self.transition(DetectorTransitions.success,tag)
                self.sendUpdate()
            else:
                if self.abort:
                    self.abort = False
                    self.transition(DetectorTransitions.abort)
                    self.inTransition = False
                    self.sendUpdate("transition aborted")
                    break
                self.transition(DetectorTransitions.error)
                self.sendUpdate("transition failed")
                self.inTransition = False
                break
        self.inTransition = False


    def executeScript(self,scriptname):
        self.scriptProcess = subprocess.Popen(["exec ./"+scriptname], shell=True)
        ret = self.scriptProcess.wait()
        if ret:
            return False
        else:
            return True

    def abortFunction(self):
        #terminate Transition if active
        if self.inTransition:
            self.abort = True
            if self.scriptProcess:
                self.scriptProcess.terminate()
        else:
            self.transition(DetectorTransitions.abort)
            self.sendUpdate()

class DetectorB(DetectorController):
    def waitForTransition(self):
        while True:
            try:
                ret = self.socketReceiver.recv_multipart()
                print(ret)
                if len(ret)  == 2:
                    command,tag = ret
                    command = command.decode()
                    tag = tag.decode()
                else:
                    command = ret[0].decode()
                    tag=None
                if self.stateMachine.currentState not in {DetectorStates.Active,DetectorStates.Unconfigured} and command != DetectorTransitions.abort:
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
                    self.transition(DetectorTransitions.configure,tag)
                    self.inTransition = True
                    self.sendUpdate()
                    workThread = threading.Thread(name="worker", target=self.getReady, args=(tag,))
                    workThread.start()
                if command == DetectorTransitions.abort:
                    self.abortFunction()
            except zmq.error.ContextTerminated:
                self.socketReceiver.close()
                break

    def getReady(self,tag):
        for i in range(0,3):
            ret = self.executeScript("detectorScript.sh")
            if ret:
                self.transition(DetectorTransitions.success,tag)
                self.sendUpdate()
            else:
                if self.abort:
                    self.abort = False
                    self.transition(DetectorTransitions.abort)
                    self.inTransition = False
                    self.sendUpdate("transition aborted")
                    break
                self.transition(DetectorTransitions.error)
                self.sendUpdate("transition failed")
                self.inTransition = False
                break
        self.inTransition = False


    def executeScript(self,scriptname):
        self.scriptProcess = subprocess.Popen(["exec ./"+scriptname], shell=True)
        ret = self.scriptProcess.wait()
        if ret:
            return False
        else:
            return True

    def abortFunction(self):
        #terminate Transition if active
        if self.inTransition:
            self.abort = True
            if self.scriptProcess:
                self.scriptProcess.terminate()
        else:
            self.transition(DetectorTransitions.abort)
            self.sendUpdate()



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
        except KeyboardInterrupt:
            test.terminate()
            break
        except EOFError:
            time.sleep(500000)
            continue
