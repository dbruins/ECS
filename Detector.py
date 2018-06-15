from Statemachine import Statemachine
import csv
from thread import start_new_thread
import zmq

class Detector:
    stateMachine = None 
    id = None
    socketSender = None
    socketReceiver = None
    # selfState -> PCAState
    mapper = {}
    def __init__(self,id,stateFile,mapFile,address):
      self.stateMachine = Statemachine(stateFile,"Shutdown",printTransitions=False)
      self.id = id
      with open(mapFile, 'rb') as file:
	  reader = csv.reader(file, delimiter=',')
	  for row in reader:
	      self.mapper[row[0]] = row[1]	
      #socket for sending Requests
      context = zmq.Context()
      self.socketSender = context.socket(zmq.REQ)
      self.socketSender.connect(("tcp://localhost:%i" % address))
    
    #request a transition to a Detector
    def transitionRequest(self,command):
      if self.stateMachine.checkIfPossible(command):
	self.socketSender.send(command)
	returnMessage = self.socketSender.recv()
	#if success transition Statemachine
	if returnMessage == "OK":
	  self.stateMachine.transition(command)
	else:
	  print "Detector returned error"
      else:
	print "command in current State not possible"
    
	  
    def getId(self):
      return self.id
    
    def getState(self):
      return self.stateMachine.currentState
    
    def getMappedState(self):
      return self.mapper[self.stateMachine.currentState]
    
    #returns the mapped next state for a command or False if the transition is invalid
    def getMappedStateForCommand(self,command):
      nextState = self.stateMachine.getNextStateForCommand(command)
      if nextState: 
	return self.mapper[nextState]
      else:
	return False
    
    def publishState(self):
      print self.mapper[self.stateMachine.currentState]
	
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
      self.configure()
    
    
    def powerOn(self):
      self.transitionRequest("poweron")
    def start(self):
      self.transitionRequest("start")
    def stop(self):
      self.transitionRequest("stop")
    def powerOff(self):
      self.transitionRequest("poweroff")
    def configure(self):
      self.transitionRequest("configure")
    def reconfigure(self):
      self.transitionRequest("reconfigure")

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