import csv
graphfile = "graph.csv"
import threading


class Statemachine:
  #currentState -> [command->newstate]
  graph = dict({})
  
  currentState = None
  
  printTransitions = None
  
  def __init__(self,csvGraph,initState, printTransitions=False):
     self.currentState = initState
     self.printTransitions = printTransitions
     #read Graph from csv file ---- | state | transition | next State |
     with open(csvGraph, 'rb') as file:
	reader = csv.reader(file, delimiter=',')
	for row in reader:
	    if row[0] in self.graph:
	      self.graph[row[0]][row[1]] = row[2]
	    else:
	      self.graph[row[0]] = dict({row[1]:row[2]})
  def transition(self,command):
    lock = threading.Lock()

    lock.acquire()
    #check if command ist valid in the current state
    if self.checkIfPossible(command):
      if self.printTransitions:
	print self.currentState + " -> " + self.graph[self.currentState][command]
      self.currentState = self.graph[self.currentState][command]
      lock.release()
      return True
    else:
      if self.printTransitions:
	print "Transition not defined/possible " + self.currentState + " command: " + command
      lock.release()
      return False
    
  def checkIfPossible(self,command):
    #print self.currentState
    #print self.graph
    if command in self.graph[self.currentState]:
      return True
    else:
      return False
    
  def publishState(self):
    print self.currentState
  
  #returns next State for given command (no transition)
  def getNextStateForCommand(self,command):
      if not self.checkIfPossible(command):
	return False
      else:
	return self.graph[self.currentState][command]
  

if __name__ == "__main__":
    test = Statemachine(graphfile,"Shutdown") 
    test.transition("poweron")
    test.transition("poweroff")
    test.transition("configure")
    test.transition("poweron")
    test.transition("configure")
    test.transition("start")
    test.transition("stop")

   
     
  