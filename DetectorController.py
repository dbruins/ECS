#!/usr/bin/python3
import zmq
import sys
import time
import threading
from ECSCodes import ECSCodes
codes = ECSCodes()
import configparser
from Statemachine import Statemachine
import json
from DataObjects import partitionDataObject, detectorDataObject, stateObject, configObject
from states import DetectorStates, DetectorTransitions, DetectorStatesB
import ECS_tools
from multiprocessing import Queue
from PartitionComponents import DetectorTypes
DetectorTypes = DetectorTypes()
DetectorTransitions = DetectorTransitions()
from BaseController import BaseController

class DetectorController(BaseController):

    def __init__(self,detectorData,startState="Unconfigured",configTag=None):
        super().__init__(detectorData.id,detectorData.portCommand)

        #Lock to handle StateMachine Transition access
        self.stateMachineLock = threading.Lock()

        config = configparser.ConfigParser()
        config.read("init.cfg")
        self.configFile = config["Default"]

        self.portCommand = detectorData.portCommand
        #seuquence Number for sending Updates
        self.sequenceNumber = 0
        self.receive_timeout = int(self.configFile['receive_timeout'])

        #get PCA Information
        pcaData = None
        while pcaData == None:
            pcaData = self.getPCAData()
        self.stateMap = ECS_tools.MapWrapper()

        confSection = DetectorTypes.getConfsectionForType(detectorData.type)
        configDet = configparser.ConfigParser()
        configDet.read("subsystem.cfg")
        configDet = configDet[confSection]
        self.stateMachine = Statemachine(configDet["stateFile"],startState)
        self.configTag = configTag
        self.config = None
        self.pcaAddress = pcaData.address
        self.pcaUpdatePort = pcaData.portUpdates
        self.pcaID = pcaData.id

        #Subscription needs its own context so we can terminate it seperately in case of a pca Change
        #self.subContext = zmq.Context()
        #self.socketSubscription = self.subContext.socket(zmq.SUB)
        #self.socketSubscription.connect("tcp://%s:%s" % (pcaData.address,pcaData.portPublish))
        #subscribe to everything
        #self.socketSubscription.setsockopt(zmq.SUBSCRIBE, b'')

        #send state to PCA
        t = threading.Thread(name='update'+str(0), target=self.sendUpdate, args=(0,))
        t.start()

        self.inTransition = False

        #self.updateThread = threading.Thread(name='waitForUpdates', target=self.waitForUpdates)
        #self.updateThread.start()
        #ECS_tools.getStateSnapshot(self.stateMap,pcaData.address,pcaData.portCurrentState,timeout=self.receive_timeout)
        self.commandThread = threading.Thread(name='waitForCommands', target=self.waitForCommands)
        self.commandThread.start()
        self.pipeThread = threading.Thread(name='waitForPipeMessages', target=self.waitForPipeMessages)
        self.pipeThread.start()

    def handleSystemMessage(self,message):
        if message == "error":
            self.error()
        elif message == "resolved":
            self.reset()
        else:
            print('received unknown message via pipe: %s' % (message,))

    def getPCAData(self):
        """get PCA Information from ECS"""
        requestSocket = self.context.socket(zmq.REQ)
        requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        requestSocket.connect("tcp://%s:%s" % (self.configFile['ECAAddress'],self.configFile['ECARequestPort']))

        requestSocket.send_multipart([codes.detectorAsksForPCA, self.MyId.encode()])
        try:
            pcaDataJSON = requestSocket.recv()
            if pcaDataJSON == codes.idUnknown:
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
        self.updateThread.join()
        self.updateThread = threading.Thread(name='waitForUpdates', target=self.waitForUpdates)
        self.updateThread.start()
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

    def handleCommand(self,message):
        """handles command common for all detectors; returns False if command is unknown"""
        arg = None
        if len(message) > 1:
            arg = message[1].decode()
        command = message[0]
        if command == codes.ping:
            self.commandSocket.send(codes.ok)
        elif command == codes.pcaAsksForDetectorStatus:
            if self.configTag:
                self.commandSocket.send_multipart([self.stateMachine.currentState.encode(),self.configTag.encode()])
            else:
                self.commandSocket.send_multipart([self.stateMachine.currentState.encode()])
        elif command == codes.detectorChangePartition:
            partition = partitionDataObject(json.loads(arg))
            self.changePCA(partition)
            self.commandSocket.send(codes.ok)
        elif command == codes.check:
            self.commandSocket.send(codes.ok)
            partition = partitionDataObject(json.loads(arg))
            if partition.id != self.pcaID:
                self.changePCA(partition)
        else:
            return False
        return True

    def terminate(self):
        self.context.term()
        #self.subContext.term()
        self.abort = True
        self.endPipeThread()
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

class DetectorA(DetectorController):

    def handleCommand(self,message):
        """handles commands for DetectorA returns False if command is unknown"""
        if not DetectorTransitions.isUserTransition(message[0].decode()):
            return super().handleCommand(message)
        #handle Transition
        if len(message)  == 2:
            command,conf = message
            command = command.decode()
            conf =  configObject(json.loads(conf.decode()))
        else:
            command = message[0].decode()
            conf=None
        if self.inTransition and command not in {DetectorTransitions.abort,DetectorTransitions.reset}:
            self.commandSocket.send(codes.busy)
            return True
        elif not self.stateMachine.checkIfPossible(command):
            self.commandSocket.send(codes.error)
            return True
        else:
            self.commandSocket.send(codes.ok)
        self.abort = False
        if command == DetectorTransitions.configure:
            self.inTransition = True
            workThread = threading.Thread(name="worker", target=self.configure, args=(conf,))
            workThread.start()
        elif command == DetectorTransitions.abort:
            self.abortFunction()
        elif command == DetectorTransitions.reset:
            self.reset()
        elif command == DetectorTransitions.error:
            self.error()
        else:
            return False
        return True

    def configure(self,conf):
        oldconfigTag = self.configTag
        self.transition(DetectorTransitions.configure,conf)
        if oldconfigTag == self.configTag:
            #nothing to be done
            self.transition(DetectorTransitions.success,conf)
            self.transition(DetectorTransitions.success,conf)
        else:
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

class STS(DetectorA):
    pass

class MVD(DetectorA):
    pass

class TRD(DetectorA):
    pass

class RICH(DetectorA):
    pass

class TOF(DetectorA):
    pass

class DetectorB(DetectorController):

    def handleCommand(self,message):
        """handles commands for DetectorB returns False if command is unknown"""
        if not DetectorTransitions.isUserTransition(message[0].decode()):
            return super().handleCommand(message)
        #handle Transition
        if len(message)  == 2:
            command,conf = message
            command = command.decode()
            conf =  configObject(json.loads(conf.decode()))
        else:
            command = message[0].decode()
            conf=None
        if self.stateMachine.currentState not in {DetectorStatesB.Active,DetectorStatesB.Unconfigured}  and command not in {DetectorTransitions.abort,DetectorTransitions.reset}:
            self.commandSocket.send(codes.busy)
            return True
        elif not self.stateMachine.checkIfPossible(command):
            self.commandSocket.send(codes.error)
            return True
        else:
            self.commandSocket.send(codes.ok)
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
        else:
            return False
        return True


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
    import getopt
    def printHelp():
        print('DetectorController.py -s <startState(optional)> -o <startConfig(optional)> <Detector Id>')
    try:
        opts, args = getopt.getopt(sys.argv[1:],"hs:c:",["help","startState","startConfig"])
    except getopt.GetoptError:
        printHelp()
        sys.exit(1)
    if len(args) < 1:
        printHelp()
        print("please enter the detector id")
        sys.exit(1)
    startState = "Unconfigured"
    startTag = None
    for opt,arg in opts:
        if opt in ("-s", "--startState"):
            startState = arg
        elif opt in ("-c", "--startConfig"):
            startTag = arg
        elif opt in ("-h", "--help"):
            printHelp()
            sys.exit(0)

    #get Detector Information
    config = configparser.ConfigParser()
    config.read("init.cfg")
    conf = config["Default"]
    detectorData = None
    while detectorData == None:
        try:
            context = zmq.Context()
            requestSocket = context.socket(zmq.REQ)
            requestSocket.connect("tcp://%s:%s" % (conf['ECAAddress'],conf['ECARequestPort']))
            requestSocket.setsockopt(zmq.RCVTIMEO, int(conf['receive_timeout']))
            requestSocket.setsockopt(zmq.LINGER,0)

            requestSocket.send_multipart([codes.getDetectorForId, args[0].encode()])
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
        test = DetectorA(detectorData,startState,startTag)
    elif detectorData.type == "DetectorB":
        test = DetectorB(detectorData,startState,startTag)
    elif detectorData.type == "TRD":
        test = TRD(detectorData,startState,startTag)
    elif detectorData.type == "STS":
        test = STS(detectorData,startState,startTag)
    elif detectorData.type == "MVD":
        test = MVD(detectorData,startState,startTag)
    elif detectorData.type == "TOF":
        test = TOF(detectorData,startState,startTag)
    elif detectorData.type == "RICH":
        test = RICH(detectorData,startState,startTag)
    else:
        raise Exception("Detector Type unknown")
    try:
        test.commandThread.join()
    except KeyboardInterrupt:
        test.terminate()
