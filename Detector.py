from Statemachine import Statemachine
import csv
from _thread import start_new_thread
import zmq
import logging
import ECSCodes
import configparser
from PCA import MapWrapper
import time

class Detector:
    def __init__(self,id,address,port,confSection,logfunction,publishQueue,pcaTimeoutFunktion,pcaReconnectFunction,active=True):
        configParser = configparser.ConfigParser()
        configParser.read("detector.cfg")
        conf = configParser[confSection]
        self.publishQueue = publishQueue
        self.active = active

        self.pcaReconnectFunction = pcaReconnectFunction
        self.pcaTimeoutFunktion = pcaTimeoutFunktion

        self.receive_timeout = int(conf["timeout"])
        self.pingIntervall = int(conf["pingIntervall"])
        self.address = ("tcp://%s:%s" % (address ,port))
        #todo get the actual status of the detector instead of init it with shutdown
        self.stateMachine = Statemachine(conf["stateFile"],"Shutdown")
        self.id = id
        self.logfunction = logfunction
        self.mapper = {}
        with open(conf["mapFile"], 'r') as file:
            reader = csv.reader(file, delimiter=',')
            for row in reader:
                self.mapper[row[0]] = row[1]
        #socket for sending Requests
        self.zmqContext = zmq.Context()
        self.socketSender = None
        self.createSendSocket()

        self.connected = None
        start_new_thread(self.ping,())

    def ping(self):
        while True:
            if not self.active:
                continue
            pingSocket = self.zmqContext.socket(zmq.REQ)
            pingSocket.connect(self.address)
            pingSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
            pingSocket.setsockopt(zmq.LINGER,0)
            pingSocket.send(ECSCodes.ping)
            try:
                r = pingSocket.recv()
                if self.connected != True:
                    self.connected = True
                    self.logfunction("Detector %s is connected" % self.id)
                    self.pcaReconnectFunction(self.id,self.getMappedState())
            except zmq.Again:
                #todo PCA should be able retrieve the actual status upon reconnect; this is just state it used to has before the reconnect
                if self.connected == True or self.connected == None:
                    self.connected = False
                    self.logfunction("timeout pinging Detector %s" % self.id, True)
                    self.pcaTimeoutFunktion(self.id)
            pingSocket.close()
            time.sleep(self.pingIntervall)

    def setActive(self):
        self.active = True

    def setInactive(self):
        self.active = False


    def createSendSocket(self):
        """init or reset the send Socket"""
        if(self.socketSender):
            #reset
            self.socketSender.close()
        self.socketSender = self.zmqContext.socket(zmq.REQ)
        self.socketSender.connect(self.address)
        self.socketSender.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        self.socketSender.setsockopt(zmq.LINGER,0)

    def transitionRequest(self,command):
        """request a transition to a Detector"""
        if not self.active:
            return False
        if self.stateMachine.checkIfPossible(command):
            self.socketSender.send(command.encode(encoding='utf_8'))
            try:
                returnMessage = self.socketSender.recv()
            except zmq.Again:
                self.logfunction("timeout from Detector "+str(self.id)+" for "+ command,True)
                #todo handle disconnect
                #del self.connectedDetectors[id]
                #reset socket
                self.createSendSocket()
                return False
            #if success transition Statemachine
            if returnMessage == ECSCodes.ok:
                oldstate = self.stateMachine.currentState
                self.stateMachine.transition(command)
                self.logfunction("Detector "+str(self.id)+" transition: "+ oldstate +" -> " + self.stateMachine.currentState)
                return True
            else:
                self.logfunction("Detector returned error",True)
                return False
        else:
            self.logfunction("command in current State not possible",True)
            return False

    def getId(self):
        return self.id

    def getState(self):
        if self.active == False:
            return "inactive"
        if self.connected == False:
            return "Connection Problem"
        return self.stateMachine.currentState

    def getMappedState(self):
        if self.active == False:
            return "inactive"
        if self.connected == False:
            return "Connection Problem"
        return self.mapper[self.stateMachine.currentState]

    def getMappedStateForCommand(self,command):
        """returns the mapped next state for a command or False if the transition is invalid"""
        nextState = self.stateMachine.getNextStateForCommand(command)
        if nextState:
            return self.mapper[nextState]
        else:
            return False

    def publishState(self):
        """most sophisticated publishing method ever made"""
        print (self.mapper[self.stateMachine.currentState])

    def getReady(self):
        pass
    def start(self):
        pass
    def stop(self):
        pass
    def powerOff(self):
        pass

class DetectorA(Detector):

    def getReady(self):
        self.powerOn()
        return self.configure()


    def powerOn(self):
        return self.transitionRequest("poweron")
    def start(self):
        return self.transitionRequest("start")
    def stop(self):
        return self.transitionRequest("stop")
    def powerOff(self):
        return self.transitionRequest("poweroff")
    def configure(self):
        return self.transitionRequest("configure")
    def reconfigure(self):
        return self.transitionRequest("reconfigure")

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
