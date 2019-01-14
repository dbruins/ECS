import PartitionComponents
from ECSCodes import ECSCodes
codes = ECSCodes()
import ECS_tools
import threading
import zmq
from multiprocessing import Queue
import time
from _thread import start_new_thread
from DataObjects import stateObject
import json
from states import DetectorStates

class UnmappedDetectorController:

    def __init__(self,detectorData,publishPort,updatePort,currentStatePort,logfunction,webSocket):
        self.detectors = ECS_tools.MapWrapper()
        self.statusMap = ECS_tools.MapWrapper()
        self.log=logfunction
        self.terminate = threading.Event()
        self.id = "unmapped"
        self.context = zmq.Context()
        self.webSocket = webSocket

        #ZMQ Socket to publish new state Updates
        self.socketPublish = self.context.socket(zmq.PUB)
        #todo the connect takes a little time messages until then will be lost
        self.socketPublish.bind("tcp://*:%s" % publishPort)
        self.publishQueue = Queue()
        self.sequence = 0

        #Socket to serve current statusMap
        self.socketServeCurrentStatus = self.context.socket(zmq.ROUTER)
        self.socketServeCurrentStatus.bind("tcp://*:%s" % currentStatePort)

        #Socket to wait for Updates From Detectors
        self.socketDetectorUpdates = self.context.socket(zmq.REP)
        self.socketDetectorUpdates.bind("tcp://*:%s" % updatePort)

        t = threading.Thread(name='publisher', target=self.publisher, args=())
        t.start()

        #Subscribers need some time to subscribe (todo there has to be a better way)
        time.sleep(1)
        #tell subscribers to reset their state Table
        self.publishQueue.put((self.id,codes.reset))

        threadArray = []
        for d in detectorData:
            t = threading.Thread(name='dcreate'+str(d.id), target=self.addDetector, args=(d,))
            threadArray.append(t)
            t.start()
        for t in threadArray:
            t.join()

        t = threading.Thread(name='updates', target=self.waitForUpdates, args=())
        t.start()

        t = threading.Thread(name='stateTable', target=self.waitStateTableRequests, args=())
        t.start()

    def publisher(self):
        while True:
            id,state = self.publishQueue.get()
            if self.terminate.is_set():
                self.socketPublish.close()
                break
            self.sequence = self.sequence + 1
            if state == codes.removed:
                del self.statusMap[id]
            elif id != self.id:
                self.statusMap[id] = (self.sequence,state)
            ECS_tools.send_status(self.socketPublish,id,self.sequence,state)
            if state == codes.reset:
                #reset code for Web Browser
                state = "reset"
            elif state == codes.removed:
                #remove code for Web Browser
                state = "remove"

            if isinstance(state,stateObject):
                state = state.asJson()

            #send update to WebUI(s)
            jsonWebUpdate = {"id" : id,
                             "state" : state,
                             "sequenceNumber" : self.sequence
                            }
            jsonWebUpdate = json.dumps(jsonWebUpdate)
            self.webSocket.sendUpdate(jsonWebUpdate,"unmapped")

    def waitForUpdates(self):
        while True:
            try:
                message = self.socketDetectorUpdates.recv()
                message = json.loads(message.decode())
                id = message["id"]
                if id not in self.detectors:
                    self.log("received message with unknown id: %s" % id,True)
                    self.socketDetectorUpdates.send(codes.idUnknown)
                    continue
                self.socketDetectorUpdates.send(codes.ok)

                det = self.detectors[id]
                state = message["state"]
                configTag = None
                comment = None
                if "tag" in message:
                    configTag = message["tag"]
                if "comment" in message:
                    comment = message["comment"]
                det.setState(state,configTag,comment)
                self.publishQueue.put((det.id,det.getStateObject()))
            except zmq.error.ContextTerminated:
                self.socketDetectorUpdates.close()
                break

    def waitStateTableRequests(self):
        """waits statetable requests"""
        while True:
            #request for entire statusMap e.g. if a new Detector/Client connects
            try:
                messsage = self.socketServeCurrentStatus.recv_multipart()

                origin = messsage[0]
                request = messsage[1]
                if request != codes.hello:
                    self.log("wrong request in socketServeCurrentStatus \n",True)
                    continue

                # send each Statusmap entry to origin
                items = self.statusMap.copy().items()
                for key, value in items:
                    #send identity of origin first
                    self.socketServeCurrentStatus.send(origin,zmq.SNDMORE)
                    #send key,sequence number,status
                    ECS_tools.send_status(self.socketServeCurrentStatus,key,value[0],value[1])
                # Final message
                self.socketServeCurrentStatus.send(origin, zmq.SNDMORE)
                ECS_tools.send_status(self.socketServeCurrentStatus,codes.done,self.sequence,b'')
            except zmq.error.ContextTerminated:
                self.socketServeCurrentStatus.close()
                break


    def handleDetectorTimeout(self,id):
        self.publishQueue.put((id,stateObject("Connection Problem")))

    def handleDetectorReconnect(self,id,stateObj):
        self.publishQueue.put((id,stateObj))

    def checkIfTypeIsKnown(self,detector):
        types = PartitionComponents.DetectorTypes()
        typeClass = types.getClassForType(detector.type)
        if not typeClass:
            return False
        return True

    def addDetector(self,detector):
        """add Detector to Dictionary and pubish it's state"""
        #create the corresponding class for the specified type
        types = PartitionComponents.DetectorTypes()
        typeClass = types.getClassForType(detector.type)
        if not typeClass:
            return False
        confSection = types.getConfsectionForType(detector.type)
        det = typeClass(detector.id,detector.address,detector.portTransition,detector.portCommand,confSection,self.log,self.handleDetectorTimeout,self.handleDetectorReconnect)
        self.detectors[det.id] = det
        self.publishQueue.put((det.id,det.getStateObject()))

        return True

    def removeDetector(self,id):
        """remove Detector from Dictionary"""
        if id not in self.detectors:
            self.log("Detector with id %s is unknown" % id,True)
            return False
        det = self.detectors[id]
        self.publishQueue.put((id,codes.removed))
        del self.detectors[id]
        #this might take a few seconds dending on ping interval
        start_new_thread(det.terminate,())
        return True

    def terminateContoller(self):
        self.terminate.set()
        self.publishQueue.put((False,False))
        self.context.term()

    def isDetectorConnected(self,detectorId):
        return self.detectors[detectorId].connected

    def abortDetector(self,detId):
        det = self.detectors[detId]
        if det.stateMachine.currentState == DetectorStates.Unconfigured:
            return True
        else:
            return det.abort()

    def isShutdown(self,detectorId):
        return self.detectors[detectorId].isShutdown()

    def shutdownDetector(self,detId):
        det = self.detectors[detId]
        if not det.powerOff():
            return False
        else:
            return True
