import csv
import threading


class Statemachine:
    def __init__(self,csvGraph,initState):
        """
        Graph Example
        {
         'Unconfigured': {'configure': 'Configuring_Step1'},
         'Configuring_Step1': {'success': 'Configuring_Step2', 'abort': 'Unconfigured', 'error': 'Unconfigured'},
         'Configuring_Step2': {'success': 'Active', 'abort': 'Unconfigured', 'error': 'Unconfigured'},
         'Active': {'configure': 'Configuring', 'abort': 'Unconfigured', 'error': 'Unconfigured'}
        }

        Graph is Dictionary with States as Keys and values as other Dictionarys with Keys as Transitions and values as Followup States
        """
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
        """transitions Statemachine with command returns True on success or False if command is not possible in current state"""
        #check if command ist valid in the current state
        if self.checkIfPossible(command):
            self.currentState = self.graph[self.currentState][command]
            return True
        else:
            return False

    def checkIfPossible(self,command):
        """checks if command is possible in current state"""
        if command in self.graph[self.currentState]:
            return True
        else:
            return False
