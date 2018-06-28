import csv
graphfile = "graph.csv"
import threading


class Statemachine:
    #currentState -> [command->newstate]
    graph = dict({})

    currentState = None

    def __init__(self,csvGraph,initState):
        self.currentState = initState
        #read Graph from csv file ---- | state | transition | next State |
        with open(csvGraph, 'r') as file:
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


if __name__ == "__main__":
    test = Statemachine(graphfile,"Shutdown")
    test.transition("poweron")
    test.transition("poweroff")
    test.transition("configure")
    test.transition("poweron")
    test.transition("configure")
    test.transition("start")
    test.transition("stop")
