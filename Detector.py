from Statemachine import Statemachine
import csv
from _thread import start_new_thread
import zmq
import logging

class Detector:
    stateMachine = None
    id = None
    socketSender = None
    socketReceiver = None

    # selfState -> PCAState
    mapper = {}
    def __init__(self,id,stateFile,mapFile,address):
        self.stateMachine = Statemachine(stateFile,"Shutdown")
        self.id = id
        with open(mapFile, 'r') as file:
            reader = csv.reader(file, delimiter=',')
            for row in reader:
                self.mapper[row[0]] = row[1]
        #socket for sending Requests
        context = zmq.Context()
        self.socketSender = context.socket(zmq.REQ)
        self.socketSender.connect(("tcp://localhost:%i" % address))


    def transitionRequest(self,command):
        """request a transition to a Detector"""
        if self.stateMachine.checkIfPossible(command):
            self.socketSender.send(command.encode(encoding='utf_8'))
            returnMessage = self.socketSender.recv_string()
            #if success transition Statemachine
            if returnMessage == "OK":
                oldstate = self.stateMachine.currentState
                self.stateMachine.transition(command)
                logging.info("Detector "+str(self.id)+" transition: "+ oldstate +" -> " + self.stateMachine.currentState)
                return True
                #self.log("GLobal Statechange: "+oldstate+" -> "+self.stateMachine.currentState)
            else:
                logging.critical("Detector returned error")
                return False
        else:
            logging.critical("command in current State not possible")
            return False

    def log(self,logmessage,error=False):
        if error:
            self.logfile.write("Error: "+logmessage)
        else:
            self.logfile.write(logmessage)

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

if __name__ == "__main__":
    d1 = DetectorA(1,"graph.csv","map.csv")
    d1.reqnsitionRequest("configure")
    x = raw_input()
