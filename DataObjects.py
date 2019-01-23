
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

    def __getitem__(self,index):
        return self.dataArray[index]

class DataObject:
    def asArray(self):
        ret = []
        for k,v in self.__dict__.items():
            ret.append(v)
        return ret

    def asJsonString(self):
        return json.dumps(self.__dict__)

    def asJson(self):
        return self.__dict__

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

class mappingDataObject(DataObject):
    def __init__(self,queryResult):
        if isinstance(queryResult,dict):
            self.detectorId = queryResult["detectorId"]
            self.partitionId = queryResult["partitionId"]
        else:
            self.detectorId = queryResult[0]
            self.partitionId = queryResult[1]

class globalSystemDataObject(DataObject):
    def __init__(self,queryResult):
        if isinstance(queryResult,dict):
            self.id = queryResult["id"]
            self.address = queryResult["address"]
            self.portCommand = queryResult["portCommand"]
        else:
            self.id = queryResult[0]
            self.address = queryResult[1]
            self.portCommand = queryResult[2]


class stateObject(DataObject):
    def asJsonString(self):
        return json.dumps(self.asJson())

    def __init__(self,data):
        self.unmappedState = None
        self.configTag = None
        self.comment = None
        if isinstance(data,dict):
            #from json
            self.state = data["state"]
            if len(data) > 1:
                self.unmappedState = data["unmappedState"]
                self.configTag = data["configTag"]
                self.comment = data["comment"]
        elif isinstance(data,list):
            self.state = data[0]
            self.unmappedState = data[1]
            if len(data) > 2:
                self.configTag = data[2]
                self.comment = data[3]
        elif isinstance(data,str):
            self.state = data
        else:
            raise TypeError("Expected dictionary or list")

class configObject(DataObject):
    def __init__(self,queryResult):
        if isinstance(queryResult,dict):
            self.configId = queryResult["configId"]
            self.systemId = queryResult["systemId"]
            self.parameters = queryResult["parameters"]
        else:
            self.configId = queryResult[0]
            self.systemId = queryResult[1]
            self.parameters = queryResult[2]
