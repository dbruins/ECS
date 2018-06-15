from Statemachine import Statemachine
import csv
from thread import start_new_thread
import zmq
import Detector
import time
import threading

class PCA:
    detectors = None
    stateMachine = None 
    commandSocket = None
    socketReceiver = None
    sem = None
    
    def __init__(self):
      self.stateMachine = Statemachine("PCAStatemachine.csv","NotReady",printTransitions=True)
      self.detectors = {}
      #ZMQ Socket for commands
      context = zmq.Context()
      self.commandSocket = context.socket(zmq.PUB)
      self.commandSocket.bind("tcp://*:5554")
      self.sem = threading.Semaphore()
      
      #socket for receiving Status Updates
      self.socketReceiver = context.socket(zmq.REP)
      self.socketReceiver.bind("tcp://*:%i" % 5553)
      start_new_thread(self.waitForMessages,())
      start_new_thread(self.checkCurrentState,())
      
    def waitForMessages(self):
      while True:
	message = self.socketReceiver.recv()
	print message
	id = None
	command = None
	
	if len(message.split()) != 2:
	   print "received empty or too long message"
	   self.socketReceiver.send("error")
	   continue
	i,command = message.split()
	if i.isdigit():
	   id = int(i)
	   
	if id == None or command == None:
	    print "received non-valid message"
	    self.socketReceiver.send("error")
	    continue
	if id not in self.detectors:
	    print "received message with unknown id"
	
	
	det = self.detectors[id]
	self.sem.acquire()
	nextMappedState = det.getMappedStateForCommand(command)
	#Detector may not start Running on it's own
	if nextMappedState and nextMappedState != "Running":
	  det.stateMachine.transition(command)
	  self.socketReceiver.send("OK")
	else:
	  print "Detector made an impossible Transition: " + command
	  self.socketReceiver.send("WTF are u doing")
	self.sem.release()
    def addDetector(self,d):
      self.detectors[d.id] = d
    
    
    def removeDetector(self,id):
      del self.detectors[id]
      
      
    def checkCurrentState(self):
      while True:
	if len(self.detectors.items()) <= 0:
	      continue
	self.sem.acquire()
	if self.stateMachine.currentState == "NotReady":
	    ready = True
	    for i,d in self.detectors.items():
	      if d.getMappedState() != "Ready":
		ready = False
	    if ready:  
		self.stateMachine.transition("configured")
	    
	if self.stateMachine.currentState == "Ready":
	    for i,d in self.detectors.items():
		#The PCA is not allowed to move into the Running State on his own
		#only ready -> not ready is possible
		if d.getMappedState() != "Ready":
		  # some Detecors are not ready anymore
		  self.error()

	if self.stateMachine.currentState == "Running":
	    for i,d in self.detectors.items():
		#Some Detector stopped Working
		if d.getMappedState() != "Running":
		  #print d.getMappedState()
		  self.error()

	if self.stateMachine.currentState == "RunningInError":
	      countDetectors = 0
	      for i,d in self.detectors.items():
		  if d.getMappedState() == "Running":
		    countDetectors = countDetectors +1
	      if countDetectors == len(self.detectors):
		#All Detecotors are working again
		self.stateMachine.transition("resolved")
	      if countDetectors == 0:
		#All detectors are dead :(
		self.stateMachine.transition("stop")
	self.sem.release()
    """
    def checkCurrentState(self):
      if self.stateMachine.currentState == "NotReady":
	ready = True
	for i,d in self.detectors.items():
	  if d.getMappedState() != "Ready":
	    ready = False
	#all detectors are ready
	if ready:  
	  self.stateMachine.transition("configured")
	return
      if self.stateMachine.currentState == "Ready":
	for i,d in self.detectors.items():
	  if d.getMappedState() != "Ready":
	    # some Detecors are not ready anymore
	    self.error()
	return
      if self.stateMachine.currentState == "Running":
	for i,d in self.detectors.items():
	  #Some Detector stopped Working
	  if d.getMappedState() != "Running":
	    self.error()
	return
      
      if self.stateMachine.currentState == "RunningInError":
	countDetectors = 0
	for i,d in self.detectors.items():
	  #All Detecotors are working again
	  if d.getMappedState() == "Running":
	    countDetectors = countDetectors +1
	
	if countDetectors == len(self.detectors):
	  #All Detecotors are working again
	  self.stateMachine.transition("resolved")
	if countDetectors == 0:
	  #All detectors are dead :(
	  self.stateMachine.transition("stop")
	
	return
    """
    
    def error(self):
      self.stateMachine.transition("error")
    
    def shutdown(self):
      for i,d in self.detectors.items():
	d.powerOff()
    
    def makeReady(self):
      for i,d in self.detectors.items():
	d.getReady()
    
    def start(self):
      #self.checkCurrentState()
      self.sem.acquire()
      if self.stateMachine.currentState != "Ready":
	print "start not possible"
      for i,d in self.detectors.items():
	d.start()
      self.stateMachine.transition("start")
      self.sem.release()

    def stop(self):
      self.sem.acquire()
      for i,d in self.detectors.items():
	  d.stop()
      self.stateMachine.transition("stop")
      self.sem.release()


if __name__ == "__main__":
  
  test = PCA()
  
  a = Detector.DetectorA(1,"graph.csv","map.csv",5555) 
  b = Detector.DetectorA(2,"graph.csv","map.csv",5556) 
  
  test.addDetector(a)
  test.addDetector(b)
  
  x = ""
  while x != "end":
      print "1: get ready"
      print "2: start"
      print "3: stop"
      print "4: shutdown"
      
      x = raw_input()
      if x == "1":
	test.makeReady()
      if x == "2":
	test.start()
      if x== "3":
	test.stop()
      if x== "4":
	test.shutdown()

  
  
  """
  while True: 
    command = raw_input()
    print command
    test.commandSocket.send(command)
  """
  """
  test = PCA()
  a = Detector.DetectorA(1,"graph.csv","map.csv") 
  b = Detector.DetectorB(2,"graph.csv","map.csv")
  d1 = test.addDetector(a)
  d2 = test.addDetector(b)

  test.start()

  a.getReady()
  b.getReady()

  test.start()

  test.stop()
  a.reconfigure()
  test.start()
  """
      
    