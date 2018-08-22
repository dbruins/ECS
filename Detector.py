from Statemachine import Statemachine
import csv
from _thread import start_new_thread
import zmq
import logging
import ECSCodes
import configparser
import time
from ECS_tools import MapWrapper
import ECS_tools

class Detector:
    def __init__(self,id,address,portTransition,portCommand,confSection,logfunction,publishQueue,pcaTimeoutFunktion,pcaReconnectFunction,putPendingTransitionFunction,removePendingTransitionFunction,active=True):
        self.terminate_bool = False
        self.transitionNumber = 0
        self.inTransition = False
        configParser = configparser.ConfigParser()
        configParser.read("detector.cfg")
        self.id = id
        conf = configParser[confSection]
        self.publishQueue = publishQueue
        self.logfunction = logfunction
        self.active = active
        self.abort_bool = False

        self.pcaReconnectFunction = pcaReconnectFunction
        self.pcaTimeoutFunktion = pcaTimeoutFunktion
        self.putPending = putPendingTransitionFunction
        self.removePendingTransition = removePendingTransitionFunction

        self.receive_timeout = int(conf["timeout"])
        self.pingIntervall = int(conf["pingIntervall"])
        self.pingAddress = ("tcp://%s:%s" % (address ,portCommand))
        self.address = ("tcp://%s:%s" % (address ,portTransition))

        #socket for sending Requests
        self.zmqContext = zmq.Context()
        self.socketSender = None
        self.createSendSocket()

        #init with the current state of the Detector
        self.connected = False
        startState = self.getStateFromDetector()
        if startState:
            self.connected = True
        self.stateMachine = Statemachine(conf["stateFile"],startState)

        self.mapper = {}
        with open(conf["mapFile"], 'r') as file:
            reader = csv.reader(file, delimiter=',')
            for row in reader:
                self.mapper[row[0]] = row[1]
        start_new_thread(self.ping,())

    def ping(self):
        while not self.terminate_bool:
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
                    if not self.stateMachine.currentState:
                        #sometimes when PCA and DC start both at once there is a timeout from getting state(maybe the socket isn't ready idk)
                        pingSocket.close()
                        continue
                    #todo there could be a race condition Problem somewhere around here
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
        self.socketSender = self.zmqContext.socket(zmq.REQ)
        self.socketSender.connect(self.address)
        self.socketSender.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        self.socketSender.setsockopt(zmq.LINGER,0)

    def transitionRequest(self,commandArray):
        """request a transition to a Detector takes an array of commands"""
        self.abort_bool = False
        for command in commandArray:
            if self.inTransition:
                print("waiting for previous Transition")
                while self.inTransition and not self.abort_bool:
                    pass
                if self.abort_bool:
                    return False

            if not self.active:
                return False
            if not self.stateMachine.checkIfPossible(command):
                self.logfunction("Transition %s is not possible for Detector %s in current state" % (command,self.id))
                return False
            self.inTransition = True
            self.transitionNumber +=1
            self.putPending(self.id,self.transitionNumber)
            self.socketSender.send_multipart([ECS_tools.intToBytes(self.transitionNumber),command.encode()])
            #check if the command has arrived
            try:
                #receive status code or new State
                returnMessage = self.socketSender.recv()
                if returnMessage == ECSCodes.busy:
                    self.logfunction("Detector %s is busy" % self.id)
                    return False
            except zmq.Again:
                self.logfunction("timeout from Detector "+str(self.id)+" for sending "+ command,True)
                self.createSendSocket()
                return False
        return True

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
        """get's the state from the dummy returns False when a Problem occurs. Use on startup or if there has been a crash or a connection Problem"""
        state = False
        requestSocket = self.zmqContext.socket(zmq.REQ)
        requestSocket.connect(self.pingAddress)
        requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        requestSocket.setsockopt(zmq.LINGER,0)
        requestSocket.send(ECSCodes.pcaAsksForDetectorStatus)
        try:
            state = requestSocket.recv().decode()
        except zmq.Again:
            self.logfunction("timeout getting Detector Status for Detector %s" % (self.id) ,True)
        except Exception as e:
            self.logfunction("error getting Detector Status for Detector %s: %s" % (self.id,str(e)) ,True)
        requestSocket.close()
        return state

    def terminate(self):
        """ stops the ping thread"""
        self.terminate_bool = True

    def getReady(self):
        pass
    def start(self):
        pass
    def stop(self):
        pass
    def powerOff(self):
        pass

    def abort(self):
        requestSocket = self.zmqContext.socket(zmq.REQ)
        requestSocket.connect(self.pingAddress)
        requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        requestSocket.setsockopt(zmq.LINGER,0)
        requestSocket.send(ECSCodes.abort)
        try:
            state = requestSocket.recv().decode()
            self.removePendingTransition(self.id)
            self.inTransition = False
            self.abort_bool = True
        except zmq.Again:
            self.logfunction("timeout aborting Detector %s" % (self.id) ,True)
        except Exception as e:
            self.logfunction("error aborting Detector %s: %s" % (self.id,str(e)) ,True)
        requestSocket.close()

class DetectorA(Detector):

    def getReady(self):
        if not (self.stateMachine.currentState == "Shutdown" or self.stateMachine.currentState == "Uncofigured"):
            self.logfunction("nothing to be done for Detector %s" % self.id)
            return False
        if self.stateMachine.currentState == "Shutdown":
            return self.transitionRequest(["poweron","configure"])
        else:
            return self.transitionRequest(["configure"])

    def powerOn(self):
        return self.transitionRequest(["poweron"])
    def start(self):
        if self.stateMachine.currentState == "Running":
            self.logfunction("nothing to be done for Detector %s" % self.id)
            return False
        return self.transitionRequest(["start"])

    def stop(self):
        if self.stateMachine.currentState != "Running":
            self.logfunction("nothing to be done for Detector %s" % self.id)
            return False
        return self.transitionRequest(["stop"])

    def powerOff(self):
        if self.stateMachine.currentState == "Shutdown":
            self.logfunction("nothing to be done for Detector %s" % self.id)
            return False
        return self.transitionRequest(["poweroff"])

    def configure(self):
        return self.transitionRequest(["configure"])
    def reconfigure(self):
        return self.transitionRequest(["reconfigure"])

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
