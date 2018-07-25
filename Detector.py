from Statemachine import Statemachine
import csv
from _thread import start_new_thread
import zmq
import logging
import ECSCodes
import configparser
import time
from ECS_tools import MapWrapper

class Detector:
    def __init__(self,id,address,port,pingPort,confSection,logfunction,publishQueue,pcaTimeoutFunktion,pcaReconnectFunction,active=True):
        configParser = configparser.ConfigParser()
        configParser.read("detector.cfg")
        self.id = id
        conf = configParser[confSection]
        self.publishQueue = publishQueue
        self.logfunction = logfunction
        self.active = active

        self.pcaReconnectFunction = pcaReconnectFunction
        self.pcaTimeoutFunktion = pcaTimeoutFunktion

        self.receive_timeout = int(conf["timeout"])
        self.pingIntervall = int(conf["pingIntervall"])
        self.pingAddress = ("tcp://%s:%s" % (address ,pingPort))
        self.address = ("tcp://%s:%s" % (address ,port))

        #socket for sending Requests
        self.zmqContext = zmq.Context()
        self.socketSender = None
        self.createSendSocket()

        context = zmq.Context()
        #todo using PAIR might by dangerous?
        socket = self.zmqContext.socket(zmq.PAIR)
        socket.connect("tcp://%s:%i" % (address,port))


        #init with the current state of the Detector
        startState = self.getStateFromDetector()
        if not startState:
            self.connected = False
        else:
            self.connected = True
        #self.currentState = startState
        self.stateMachine = Statemachine(conf["stateFile"],startState)

        self.mapper = {}
        with open(conf["mapFile"], 'r') as file:
            reader = csv.reader(file, delimiter=',')
            for row in reader:
                self.mapper[row[0]] = row[1]

        start_new_thread(self.ping,())

    def ping(self):
        while True:
            if not self.active:
                continue
            pingSocket = self.zmqContext.socket(zmq.REQ)
            pingSocket.connect(self.pingAddress)
            pingSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
            pingSocket.setsockopt(zmq.LINGER,0)
            pingSocket.send(ECSCodes.ping)
            try:
                r = pingSocket.recv()
                if self.connected != True:
                    self.logfunction("Detector %s is connected" % self.id)
                    #todo need to check wether the transitions made during connection Problem were valid
                    self.stateMachine.currentState = self.getStateFromDetector()
                    self.connected = True
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
        self.socketSender = self.zmqContext.socket(zmq.PAIR)
        self.socketSender.connect(self.address)
        self.socketSender.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        self.socketSender.setsockopt(zmq.LINGER,0)

    def transitionRequest(self,command):
        """request a transition to a Detector"""
        if not self.active:
            return False
        if not self.stateMachine.checkIfPossible(command):
            self.logfunction("Transition %s is not possible for Detector %s in current state" % (command,self.id))
            return False
        self.socketSender.send(command.encode())
        #check if the command has arrived
        try:
            returnMessage = self.socketSender.recv()
        except zmq.Again:
            self.logfunction("timeout from Detector "+str(self.id)+" for sending "+ command,True)
            return False
        #get transition result
        try:
            returnMessage = self.socketSender.recv()
            if returnMessage != ECSCodes.error:
                oldstate = self.stateMachine.currentState
                self.stateMachine.currentState = returnMessage.decode()
                self.logfunction("Detector "+str(self.id)+" transition: "+ oldstate +" -> " + self.stateMachine.currentState)
                return True
            else:
                self.logfunction("Detector returned error",True)
                return False
        except zmq.Again:
            self.logfunction("timeout from Detector "+str(self.id)+" for command "+ command,True)
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

    def getStateFromDetector(self):
        """get's the state from the dummy. Use if there has been a crash or a connection Problem"""
        self.socketSender.send(ECSCodes.pcaAsksForDetectorStatus)
        state = False
        try:
            state = self.socketSender.recv().decode()
        except zmq.Again:
            #reset Socket
            self.createSendSocket()
            self.logfunction("timeout getting Detector Status for Detector %s" % (self.id) ,True)
        return state

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
        if not (self.stateMachine.currentState == "Shutdown" or self.stateMachine.currentState == "Uncofigured"):
            return False
        if self.stateMachine.currentState == "Shutdown":
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
