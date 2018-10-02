import csv
import threading


class Statemachine:
    def __init__(self,csvGraph,initState):
        #currentState -> [command->newstate]
        self.graph = dict({})

        self.currentState = initState
        #read Graph from csv file ---- | state | transition | next State |
        with open(csvGraph, 'r') as file:
            reader = csv.reader(file, delimiter=',')
            for row in reader:
                if len(row) == 3:
                    if row[0] in self.graph:
                        self.graph[row[0]][row[1]] = row[2]
                    else:
                        self.graph[row[0]] = dict({row[1]:row[2]})

    def transition(self,command):
        lock = threading.Lock()
        lock.acquire()
        #check if command ist valid in the current state
        if self.checkIfPossible(command):
            self.currentState = self.graph[self.currentState][command]
            lock.release()
            return True
        else:
            lock.release()
            return False

    def checkIfPossible(self,command):
        if command in self.graph[self.currentState]:
            return True
        else:
            return False

    #returns next State for given command (no transition)
    def getNextStateForCommand(self,command):
        if not self.checkIfPossible(command):
            return False
        else:
            return self.graph[self.currentState][command]
