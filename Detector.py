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
    def __init__(self,id,confSection,logfunction,connectedDetectors):
        configParser = configparser.ConfigParser()
        configParser.read("detector.cfg")
        conf = configParser[confSection]
        self.connectedDetectors = connectedDetectors

        basePort = conf["basePort"]
        myPort = int(basePort) + id
        print (myPort)
        self.receive_timeout = int(conf["timeout"])
        #todo localhost is hardcoded
        self.address = ("tcp://localhost:%i" % myPort)
        self.stateMachine = Statemachine(conf["stateFile"],"Shutdown")
        self.id = id
        self.logfunction = logfunction
        with open(conf["mapFile"], 'r') as file:
            reader = csv.reader(file, delimiter=',')
            for row in reader:
                self.mapper[row[0]] = row[1]
        #socket for sending Requests
        self.zmqContext = zmq.Context()
        self.createSendSocket()

        start_new_thread(self.ping,())


    def ping(self):
        while True:
            pingSocket = self.zmqContext.socket(zmq.REQ)
            pingSocket.connect(self.address)
            pingSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
            pingSocket.setsockopt(zmq.LINGER,0)
            pingSocket.send(ECSCodes.ping)
            try:
                r = pingSocket.recv()
                if not (self.id in self.connectedDetectors):
                    self.logfunction("Detector %s is connected" % self.id)
                    #todo kind of stupid
                    self.connectedDetectors[self.id] = self.id
            except zmq.Again:
                self.logfunction("timeout pinging Detector %s" % self.id, True)
                #todo PCA should be able retrieve the actual status upon reconnect
                if self.id in self.connectedDetectors:
                    self.stateMachine.transition("poweroff")
                    self.publishQueue.put((self.id,self.getMappedState()))
                del self.connectedDetectors[self.id]
            pingSocket.close()
            time.sleep(self.pingIntervall)

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
        if self.stateMachine.checkIfPossible(command):
            if not self.id in self.connectedDetectors:
                self.logfunction("Detector %s is not connected" % self.id)
                return False
            self.socketSender.send(command.encode(encoding='utf_8'))
            try:
                returnMessage = self.socketSender.recv()
            except zmq.Again:
                self.logfunction("timeout from Detector "+str(self.id)+" for "+ command,True)
                del self.connectedDetectors[id]
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
        return self.stateMachine.currentState

    def getMappedState(self):
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
    def __init__(self,id,address,port,logfunction,connectedDetectors,publishQueue):
        self.connectedDetectors = connectedDetectors
        self.publishQueue = publishQueue
        configParser = configparser.ConfigParser()
        configParser.read("detector.cfg")
        conf = configParser["DETECTOR_A"]

        self.receive_timeout = int(conf["timeout"])
        self.pingIntervall = int(conf["pingIntervall"])
        self.address = ("tcp://%s:%s" % (address ,port))
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

        start_new_thread(self.ping,())

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

    def getClassForType(self,type):
        if type in self.typeList:
            return self.typeList[type]
        else:
            return None
