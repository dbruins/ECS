from Statemachine import Statemachine
import csv
from _thread import start_new_thread
import zmq
import logging
import ECSCodes
import configparser
import time
import ECS_tools
import threading

class Detector:
    def __init__(self,id,address,portTransition,portCommand,confSection,logfunction,publishQueue,pcaTimeoutFunktion,pcaReconnectFunction,putPendingTransitionFunction,removePendingTransitionFunction,active=True):
        self.transitionNumber = 0
        self.inTransition = False
        configParser = configparser.ConfigParser()
        configParser.read("detector.cfg")
        self.id = id
        conf = configParser[confSection]
        self.publishQueue = publishQueue
        self.logfunction = logfunction
        self.active = threading.Event()
        if active:
            self.active.set()
        self.abort_bool = False

        self.pcaReconnectFunction = pcaReconnectFunction
        self.pcaTimeoutFunktion = pcaTimeoutFunktion
        self.putPending = putPendingTransitionFunction
        self.removePendingTransition = removePendingTransitionFunction

        self.receive_timeout = int(conf["timeout"])
        self.pingInterval = int(conf["pingInterval"])
        self.pingAddress = ("tcp://%s:%s" % (address ,portCommand))
        self.transitionAddress = ("tcp://%s:%s" % (address ,portTransition))
        self.address = address

        #zmq Context for Detector
        self.zmqContext = zmq.Context()


        #ping Thread will set Statemachine on connection
        self.connected = False
        self.stateMachine = Statemachine(conf["stateFile"],False)

        self.mapper = {}
        with open(conf["mapFile"], 'r') as file:
            reader = csv.reader(file, delimiter=',')
            for row in reader:
                self.mapper[row[0]] = row[1]
        start_new_thread(self.ping,())

    def ping(self):
        while True:
            if not self.active.isSet():
                #hold pings while inactive
                self.active.wait()
                if self.zmqContext.closed:
                    pingSocket.close()
                    break
            try:
                #for whatever reason this raises a different Exception for ContextTerminated than send or recv
                pingSocket = self.zmqContext.socket(zmq.REQ)
            except zmq.error.ZMQError:
                pingSocket.close()
                break
            try:
                pingSocket.connect(self.pingAddress)
                pingSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
                pingSocket.setsockopt(zmq.LINGER,0)
                pingSocket.send(ECSCodes.ping)
                pingSocket.recv()
                if self.connected != True:
                    self.logfunction("Detector %s is connected" % self.id)
                    #todo need to check wether the transitions made during connection Problem were valid
                    self.stateMachine.currentState = self.getStateFromDetector()
                    if not self.stateMachine.currentState:
                        #sometimes when PCA and DC start both at once there is a timeout from getting state(maybe the socket isn't ready idk)
                        continue
                    #todo there could be a race condition Problem somewhere around here
                    self.connected = True
                    self.pcaReconnectFunction(self.id,self.getMappedState())
            except zmq.Again:
                if self.connected == True or self.connected == None:
                    self.connected = False
                    self.logfunction("timeout pinging Detector %s" % self.id, True)
                    self.pcaTimeoutFunktion(self.id)
            except zmq.error.ContextTerminated:
                #termination during sending ping
                break
            finally:
                pingSocket.close()
            time.sleep(self.pingInterval)

    def setActive(self):
        self.active.set()

    def setInactive(self):
        self.active.clear()

    def createSendSocket(self):
        """init or reset the send Socket"""
        socketSender = self.zmqContext.socket(zmq.REQ)
        socketSender.connect(self.transitionAddress)
        socketSender.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        socketSender.setsockopt(zmq.LINGER,0)
        return socketSender

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

            if not self.active.isSet() or not self.stateMachine.currentState:
                return False
            if not self.stateMachine.checkIfPossible(command):
                self.logfunction("Transition %s is not possible for Detector %s in current state" % (command,self.id))
                return False
            self.inTransition = True
            self.transitionNumber +=1
            self.putPending(self.id,self.transitionNumber)
            try:
                socketSender = self.createSendSocket()
                socketSender.send_multipart([ECS_tools.intToBytes(self.transitionNumber),command.encode()])
                #check if the command has arrived
                #receive status code or new State
                returnMessage = socketSender.recv()
                if returnMessage == ECSCodes.busy:
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

    def getId(self):
        return self.id

    def getState(self):
        if not self.active.isSet():
            return "inactive"
        if not self.connected:
            return "Connection Problem"
        return self.stateMachine.currentState

    def getMappedState(self):
        if not self.active.isSet():
            return "inactive"
        if not self.connected:
            return "Connection Problem"
        return self.mapper[self.stateMachine.currentState]

    def getStateFromDetector(self):
        """get's the state from the dummy returns False when a Problem occurs. Use on startup or if there has been a crash or a connection Problem"""
        state = False
        try:
            requestSocket = self.zmqContext.socket(zmq.REQ)
            requestSocket.connect(self.pingAddress)
            requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
            requestSocket.setsockopt(zmq.LINGER,0)
            requestSocket.send(ECSCodes.pcaAsksForDetectorStatus)
            state = requestSocket.recv().decode()
        except zmq.Again:
            self.logfunction("timeout getting Detector Status for Detector %s" % (self.id) ,True)
        except Exception as e:
            self.logfunction("error getting Detector Status for Detector %s: %s" % (self.id,str(e)) ,True)
        finally:
            requestSocket.close()
        return state

    def terminate(self):
        """ stops the ping thread"""
        self.setActive()
        self.zmqContext.term()
        self.logfunction("Detector "+str(self.id)+" was terminated",True)

    def getReady(self):
        pass
    def start(self):
        pass
    def stop(self):
        pass
    def powerOff(self):
        pass
    def isShutdown(self):
        pass

    def abort(self):
        try:
            requestSocket = self.zmqContext.socket(zmq.REQ)
            requestSocket.connect(self.pingAddress)
            requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
            requestSocket.setsockopt(zmq.LINGER,0)
            requestSocket.send(ECSCodes.abort)
            state = requestSocket.recv().decode()
            self.removePendingTransition(self.id)
            self.inTransition = False
            self.abort_bool = True
        except zmq.Again:
            self.logfunction("timeout aborting Detector %s" % (self.id) ,True)
            return False
        except Exception as e:
            self.logfunction("error aborting Detector %s: %s" % (self.id,str(e)) ,True)
            return False
        finally:
            requestSocket.close()
        return True

class DetectorA(Detector):

    def getReady(self):
        if not (self.isShutdown() or self.stateMachine.currentState == "Uncofigured"):
            self.logfunction("nothing to be done for Detector %s" % self.id)
            return False
        if self.isShutdown():
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
        if self.isShutdown():
            self.logfunction("nothing to be done for Detector %s" % self.id)
            return False
        return self.transitionRequest(["poweroff"])

    def configure(self):
        return self.transitionRequest(["configure"])
    def reconfigure(self):
        return self.transitionRequest(["reconfigure"])

    def isShutdown(self):
        if self.getState() == "Shutdown":
            return True
        else:
            return False

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
