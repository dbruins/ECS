
import json

class DataObjectCollection:
    """class for storing many database entrys"""

    def __init__(self,data,dataClass):
        self.dataArray = []
        self.dataClass = dataClass
        """create collection from a list or a dictionary/json"""
        if isinstance(data, list):
            for d in data:
                self.dataArray.append(dataClass(d))
        elif isinstance(data,dict):
            for k,v in data.items():
                #turn Dictionary entrys into lists
                dataList = [k]
                dataList.extend(v)
                self.dataArray.append(dataClass(dataList))
        else:
            raise TypeError("expected a list or a dictionary")

    def add(self,obj):
        if isinstance(obj,self.dataClass):
            self.dataArray.append(obj)
        else:
            raise TypeError("argument has wrong type")

    def asDictionary(self):
        """returns data in a dictionary(id->[data])"""
        dict = {}
        for d in self.dataArray:
            d = d.asArray()
            #id should be first entry
            dict[d[0]] = d[1:]
        return dict

    def asJsonString(self):
        dict = self.asDictionary()
        return json.dumps(dict)

    def __iter__(self):
        return self.dataArray.__iter__()

class DataObject:
    def asArray(self):
        ret = []
        for k,v in self.__dict__.items():
            ret.append(v)
        return ret

    def asJsonString(self):
        return json.dumps(self.__dict__)

    def __str__(self):
        return str(self.__dict__)

class detectorDataObject(DataObject):
    """class for storing a detector database entry"""
    def __init__(self,queryResult):
        if isinstance(queryResult,dict):
            self.id = queryResult["id"]
            self.address = queryResult["address"]
            self.type = queryResult["type"]
            self.portTransition = queryResult["portTransition"]
            self.portCommand = queryResult["portCommand"]
        else:
            self.id = queryResult[0]
            self.address = queryResult[1]
            self.type = queryResult[2]
            self.portTransition = queryResult[3]
            self.portCommand = queryResult[4]

class partitionDataObject(DataObject):
    """class for storing a Partition database entry"""
    def __init__(self,queryResult):
        if isinstance(queryResult,dict):
            self.id = queryResult["id"]
            self.address = queryResult["address"]
            self.portPublish = queryResult["portPublish"]
            self.portLog = queryResult["portLog"]
            self.portUpdates= queryResult["portUpdates"]
            self.portCurrentState = queryResult["portCurrentState"]
            self.portSingleRequest = queryResult["portSingleRequest"]
            self.portCommand = queryResult["portCommand"]
        else:
            self.id = queryResult[0]
            self.address = queryResult[1]
            self.portPublish = queryResult[2]
            self.portLog = queryResult[3]
            self.portUpdates= queryResult[4]
            self.portCurrentState = queryResult[5]
            self.portSingleRequest = queryResult[6]
            self.portCommand = queryResult[7]

class transitionObject(DataObject):
    def __init__(self,state):
        if isinstance(state,dict):
            self.transitionNumber = int(state["transitionNumber"])
            self.transitionName = state["transitionName"]
            self.nextState = state["nextState"]
        else:
            self.transitionNumber = state[0]
            self.transitionName = state[1]
            self.nextState = state[2]
from json import JSONEncoder
class stateObject(DataObject):

    def asJsonString(self):
        if self.transition:
            return json.dumps(dict({
                "state" : self.state,
                "transition" : self.transition.asJsonString(),
            }))
        else:
            return json.dumps(dict({
                "state" : self.state,
                "transition" : None,
            }))

    def __init__(self,data):
        if isinstance(data,dict):
            #from json
            self.state = data["state"]
            if isinstance(data["transition"],str):
                self.transition = transitionObject(json.loads(data["transition"]))
            else:
                self.transition = data["transition"]
        elif isinstance(data,str):
            self.state = data
            self.transition = None
        elif isinstance(data,list):
            if len(data) == 2:
                #state String + transitionObject
                self.state = data[0]
                self.transition = data[1]
            else:
                self.state = data[0]
                self.transition = transitionObject(data[1:])
        else:
            raise TypeError("Expected dictionary string or list")
