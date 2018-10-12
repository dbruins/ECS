from Statemachine import Statemachine
import csv
from _thread import start_new_thread
import zmq
import logging
from ECSCodes import ECSCodes
codes = ECSCodes()
from states import DCSStates,DCSTransitions, DetectorStates, DetectorTransitions, FLESStates, FLESTransitions, QAStates, QATransitions, TFCStates, TFCTransitions, GlobalSystemStates, GlobalSystemStatesTransitions
import configparser
import time
import ECS_tools
import threading
from DataObjects import stateObject
import json
class PartitionComponent:
    def __init__(self,address,portCommand,confSection,logfunction,pcaTimeoutFunction,pcaReconnectFunction):
        configParser = configparser.ConfigParser()
        configParser.read("detector.cfg")
        conf = configParser[confSection]
        self.logfunction = logfunction
        self.abort_bool = False
        self.currentStateObject = None

        self.pcaReconnectFunction = pcaReconnectFunction
        self.pcaTimeoutFunction = pcaTimeoutFunction

        self.receive_timeout = int(conf["timeout"])
        self.pingInterval = int(conf["pingInterval"])
        self.commandAddress = ("tcp://%s:%s" % (address ,portCommand))

        #zmq Context for Detector
        self.zmqContext = zmq.Context()


        #ping Thread will set Statemachine on connection
        self.connected = False
        self.stateMachine = Statemachine(conf["stateFile"],False)

        self.mapper = {}
        with open(conf["mapFile"], 'r') as file:
            reader = csv.reader(file, delimiter=',')
            for row in reader:
                if len(row) == 2:
                    self.mapper[row[0]] = row[1]
        start_new_thread(self.ping,())


    def ping(self):
        while True:
            try:
                #for whatever reason this raises a different Exception for ContextTerminated than send or recv
                pingSocket = self.zmqContext.socket(zmq.REQ)
            except zmq.error.ZMQError:
                pingSocket.close()
                break
            try:
                pingSocket.connect(self.commandAddress)
                pingSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
                pingSocket.setsockopt(zmq.LINGER,0)
                pingSocket.send(codes.ping)
                pingSocket.recv()
                if self.connected != True:
                    self.logfunction("%s is connected" % self.name)
                    ret = self.getStateFromDetector()
                    if not ret:
                        #sometimes when PCA and DC start both at once there is a timeout from getting state(maybe the socket isn't ready idk)
                        continue
                    state,configTag = ret
                    self.connected = True
                    self.setState(state,configTag,None)
                    self.reconnectFunction()
            except zmq.Again:
                if self.connected == True or self.connected == None:
                    self.connected = False
                    self.logfunction("timeout pinging %s" % self.name, True)
                    self.timeoutFunction()
            except zmq.error.ContextTerminated:
                #termination during sending ping
                break
            finally:
                pingSocket.close()
            time.sleep(self.pingInterval)

    def setState(self,state,configTag,Comment):
        self.stateMachine.currentState = state
        self.currentStateObject = stateObject([self.getMappedState(),state,configTag,Comment])


class Detector(PartitionComponent):

    def __init__(self,id,address,portTransition,portCommand,confSection,logfunction,pcaTimeoutFunction,pcaReconnectFunction):
        self.id = id
        self.transitionAddress = ("tcp://%s:%s" % (address ,portTransition))
        self.name = "Detector %s" % id
        super().__init__(address,portCommand,confSection,logfunction,pcaTimeoutFunction,pcaReconnectFunction)

    def reconnectFunction(self):
        self.pcaReconnectFunction(self.id,self.getStateObject())

    def timeoutFunction(self):
        self.pcaTimeoutFunction(self.id)

    def getId(self):
        return self.id

    def getState(self):
        if not self.connected:
            return DetectorStates.ConnectionProblem
        return self.stateMachine.currentState

    def getMappedState(self):
        if not self.connected:
            return DetectorStates.ConnectionProblem
        return self.mapper[self.stateMachine.currentState]

    def getStateObject(self):
        """gets current state + transition for Publishing"""
        if not self.connected:
            return stateObject(self.getMappedState())
        else:
            return self.currentStateObject

    def createSendSocket(self):
        """init or reset the send Socket"""
        socketSender = self.zmqContext.socket(zmq.REQ)
        socketSender.connect(self.transitionAddress)
        socketSender.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        socketSender.setsockopt(zmq.LINGER,0)
        return socketSender

    def transitionRequest(self,command,configTag=None):
        """request a transition from a Detector"""
        self.abort_bool = False
        if not self.connected:
            self.logfunction("Can't transition because Detector %s isn't connected" % self.id)
            return False
        if not self.stateMachine.checkIfPossible(command):
            self.logfunction("Transition %s is not possible for Detector %s in current state" % (command,self.id))
            return False
        try:
            socketSender = self.createSendSocket()
            if configTag != None:
                socketSender.send_multipart([command.encode(),configTag.encode()])
            else:
                socketSender.send_multipart([command.encode()])
            #check if the command has arrived
            #receive status code
            returnMessage = socketSender.recv()
            if returnMessage == codes.busy:
                self.logfunction("Detector %s is busy" % self.id)
                return False
        except zmq.Again:
            self.logfunction("timeout from Detector "+str(self.id)+" for sending "+ command,True)
            return False
        except zmq.error.ContextTerminated:
            self.logfunction("Detector "+str(self.id)+" was terminated during "+ command,True)
            return False
        finally:
            socketSender.close()
        return True

    def getStateFromDetector(self):
        """get's the state from the DetectorController eturns False when a Problem occurs. Use on startup or if there has been a crash or a connection Problem"""
        state = False
        try:
            requestSocket = self.zmqContext.socket(zmq.REQ)
            requestSocket.connect(self.commandAddress)
            requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
            requestSocket.setsockopt(zmq.LINGER,0)
            requestSocket.send(codes.pcaAsksForDetectorStatus)
            ret = requestSocket.recv_multipart()
            ret = list(map(lambda x:x.decode(),ret))
            configTag = None
            if len(ret) > 1:
                state,configTag = ret
            else:
                state = ret[0]
            return (state,configTag)
        except zmq.Again:
            self.logfunction("timeout getting Detector Status for Detector %s" % (self.id) ,True)
        except Exception as e:
            self.logfunction("error getting Detector Status for Detector %s: %s" % (self.id,str(e)) ,True)
        finally:
            requestSocket.close()

    def terminate(self):
        """ stops the ping thread"""
        self.zmqContext.term()
        self.logfunction("Detector "+str(self.id)+" was terminated",True)

class DetectorA(Detector):

    def getReady(self,configTag):
        if self.getMappedState() not in {DetectorStates.Unconfigured, DetectorStates.ConnectionProblem}:
            self.logfunction("nothing to be done for Detector %s" % self.id)
            return True
        return self.transitionRequest(DetectorTransitions.configure,configTag)

    def abort(self):
        return self.transitionRequest(DetectorTransitions.abort)

class GlobalSystemComponent(PartitionComponent):
    def __init__(self,pcaId,address,portCommand,confSection,logfunction,pcaTimeoutFunction,pcaReconnectFunction):
        self.pcaId = pcaId
        super().__init__(address,portCommand,confSection,logfunction,pcaTimeoutFunction,pcaReconnectFunction)

    def getState(self):
        if not self.connected:
            return GlobalSystemStates.ConnectionProblem
        return self.stateMachine.currentState

    def getMappedState(self):
        if not self.connected:
            return GlobalSystemStates.ConnectionProblem
        return self.mapper[self.stateMachine.currentState]

    def getStateObject(self):
        """gets current state + transition for Publishing"""
        if not self.connected:
            return stateObject(self.getMappedState())
        else:
            return self.currentStateObject

    def transitionRequest(self,command,configTag=None):
        self.abort_bool = False
        if not self.connected:
            self.logfunction("Can't transition because %s isn't connected" % self.name)
            return False
        if not self.stateMachine.currentState:
            return False
        if not self.stateMachine.checkIfPossible(command):
            self.logfunction("Transition %s is not possible for %s in current state" % (command,self.name))
            return False
        try:
            socketSender = self.zmqContext.socket(zmq.REQ)
            socketSender.connect(self.commandAddress)
            socketSender.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
            socketSender.setsockopt(zmq.LINGER,0)
            if configTag != None:
                socketSender.send_multipart([command.encode(),self.pcaId.encode(),configTag.encode()])
            else:
                socketSender.send_multipart([command.encode(),self.pcaId.encode()])
            #check if the command has arrived
            #receive status code
            returnMessage = socketSender.recv()
            print(returnMessage)
            if returnMessage == codes.busy:
                self.logfunction("%s is busy" % self.name)
                return False
        except zmq.Again:
            self.logfunction("timeout from "+str(self.name)+" for sending "+ command,True)
            return False
        except zmq.error.ContextTerminated:
            self.logfunction(str(self.name)+" was terminated during "+ command,True)
            return False
        finally:
            socketSender.close()
        return True

    def getStateFromDetector(self):
        state = False
        try:
            requestSocket = self.zmqContext.socket(zmq.REQ)
            requestSocket.connect(self.commandAddress)
            requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
            requestSocket.setsockopt(zmq.LINGER,0)
            requestSocket.send_multipart([codes.pcaAsksForDetectorStatus,self.pcaId.encode()])
            ret = requestSocket.recv_multipart()
            ret = list(map(lambda x:x.decode(),ret))
            configTag = None
            if len(ret) > 1:
                state,configTag = ret
            else:
                state = ret[0]
            return (state,configTag)
        except zmq.Again:
            self.logfunction("timeout getting Status for %s" % (self.name) ,True)
        except Exception as e:
            self.logfunction("error getting Status for %s" % (self.name,str(e)) ,True)
            raise e
        finally:
            requestSocket.close()

class DCS(GlobalSystemComponent):
    def __init__(self,pcaId,address,portCommand,confSection,logfunction,pcaTimeoutFunction,pcaReconnectFunction):
        self.name = "DCS"
        super().__init__(pcaId,address,portCommand,confSection,logfunction,pcaTimeoutFunction,pcaReconnectFunction)

    def reconnectFunction(self):
        print("DCS connect")
        self.pcaTimeoutFunction(self.name)

    def timeoutFunction(self):
        print("DCS disconnect")
        self.pcaReconnectFunction(self.name)

    def getReady(self,configTag):
        if not (self.stateMachine.currentState == DCSStates.Unconfigured) and self.connected:
            self.logfunction("nothing to be done for %s" % self.name)
            return True
        return self.transitionRequest(DCSTransitions.configure,configTag)

    def abort(self):
        return self.transitionRequest(DCSTransitions.abort)

class TFC(GlobalSystemComponent):
    def __init__(self,pcaId,address,portCommand,confSection,logfunction,pcaTimeoutFunction,pcaReconnectFunction):
        self.name = "TFC"
        super().__init__(pcaId,address,portCommand,confSection,logfunction,pcaTimeoutFunction,pcaReconnectFunction)

    def reconnectFunction(self):
        print("TFC connect")
        self.pcaTimeoutFunction(self.name)

    def timeoutFunction(self):
        print("TFC disconnect")
        self.pcaReconnectFunction(self.name)

    def getReady(self,configTag):
        if not (self.stateMachine.currentState == TFCStates.Unconfigured) and self.connected:
            self.logfunction("nothing to be done for %s" % self.name)
            return True
        return self.transitionRequest(TFCTransitions.configure,configTag)

    def abort(self):
        return self.transitionRequest(TFCTransitions.abort)

class QA(GlobalSystemComponent):
    def __init__(self,pcaId,address,portCommand,confSection,logfunction,pcaTimeoutFunction,pcaReconnectFunction):
        self.name = "QA"
        super().__init__(pcaId,address,portCommand,confSection,logfunction,pcaTimeoutFunction,pcaReconnectFunction)

    def reconnectFunction(self):
        print("QA connect")
        self.pcaTimeoutFunction(self.name)

    def timeoutFunction(self):
        print("QA disconnect")
        self.pcaReconnectFunction(self.name)

    def startRecording(self):
        if self.stateMachine.currentState == QAStates.Active:
            return self.transitionRequest(QATransitions.start)
        return False

    def stopRecording(self):
        if self.stateMachine.currentState == QAStates.Recording:
            return self.transitionRequest(QATransitions.stop)
        return False

    def getReady(self,configTag):
        if not (self.stateMachine.currentState == QAStates.Unconfigured) and self.connected:
            self.logfunction("nothing to be done for %s" % self.name)
            return True
        return self.transitionRequest(QATransitions.configure,configTag)

    def abort(self):
        return self.transitionRequest(QATransitions.abort)

class FLES(GlobalSystemComponent):
    def __init__(self,pcaId,address,portCommand,confSection,logfunction,pcaTimeoutFunction,pcaReconnectFunction):
        self.name = "FLES"
        super().__init__(pcaId,address,portCommand,confSection,logfunction,pcaTimeoutFunction,pcaReconnectFunction)

    def reconnectFunction(self):
        print("FLES connect")
        self.pcaTimeoutFunction(self.name)

    def timeoutFunction(self):
        print("FLES disconnect")
        self.pcaReconnectFunction(self.name)

    def startRecording(self):
        if self.stateMachine.currentState == FLESStates.Active:
            return self.transitionRequest(FLESTransitions.start)
        return False

    def stopRecording(self):
        if self.stateMachine.currentState == FLESStates.Recording:
            return self.transitionRequest(FLESTransitions.stop)
        return False

    def getReady(self,configTag):
        if not (self.stateMachine.currentState == FLESStates.Unconfigured) and self.connected:
            self.logfunction("nothing to be done for %s" % self.name)
            return True
        return self.transitionRequest(FLESTransitions.configure,configTag)

    def abort(self):
        return self.transitionRequest(FLESTransitions.abort)


class DetectorB(Detector):
    def getReady(self):
        self.powerOn()
        self.configure()

    def powerOn(self):
        self.stateMachine.transition("poweron")
    def start(self):
        self.stateMachine.transition("start")
    def stop(self):
        self.stateMachine.transition("stop")
    def powerOff(self):
        self.stateMachine.transition("poweroff")
    def configure(self):
        self.stateMachine.transition("configure")
    def reconfigure(self):
        self.stateMachine.transition("reconfigure")

class DetectorTypes:
    typeList = {
        "DetectorA" : DetectorA,
        "DetectorB" : DetectorB,
    }

    confSection = {
        "DetectorA" : "DETECTOR_A",
        "DetectorB" : "DETECTOR_B",
    }

    def getClassForType(self,type):
        if type in self.typeList:
            return self.typeList[type]
        else:
            return None

    def getConfsectionForType(self,type):
        if type in self.confSection:
            return self.confSection[type]
        else:
            return None
