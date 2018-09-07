import Detector
import ECSCodes
import ECS_tools
import threading
import zmq
from multiprocessing import Queue
import time
from _thread import start_new_thread

class UnmappedDetectorController:

    def __init__(self,detectorData,publishPort,updatePort,currentStatePort,logfunction):
        self.detectors = ECS_tools.MapWrapper()
        self.statusMap = ECS_tools.MapWrapper()
        self.log=logfunction
        self.terminate = threading.Event()
        self.id = "unmapped"
        self.context = zmq.Context()

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
        self.publishQueue.put((self.id,ECSCodes.reset))

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
            if state == ECSCodes.removed:
                del self.statusMap[id]
            elif id != self.id:
                self.statusMap[id] = (self.sequence,state)
            ECS_tools.send_status(self.socketPublish,id,self.sequence,state)

    def waitForUpdates(self):
        while True:
            try:
                message = self.socketDetectorUpdates.recv_multipart()
                print (message)
                if len(message) != 3:
                    self.log("received too short or too long message: %s" % message,True)
                    continue
                id, transitionNumber, state = message
                id = id.decode()
                state = state.decode()

                if id not in self.detectors:
                    self.log("received message with unknown id: %s" % id,True)
                    self.socketDetectorUpdates.send(ECSCodes.idUnknown)
                    continue
                self.socketDetectorUpdates.send(ECSCodes.ok)

                det = self.detectors[id]
                det.stateMachine.currentState = state
                self.publishQueue.put((det.id,det.getMappedState()))
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
                if request != ECSCodes.hello:
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
                ECS_tools.send_status(self.socketServeCurrentStatus,ECSCodes.done,self.sequence,b'')
            except zmq.error.ContextTerminated:
                self.socketServeCurrentStatus.close()
                break


    def handleDetectorTimeout(self,id):
        self.publishQueue.put((id,"Connection Problem"))

    def handleDetectorReconnect(self,id,state):
        self.publishQueue.put((id,state))

    def dummyFunction(self,a,b=None):
        pass

    def addDetector(self,detector):
        """add Detector to Dictionary and pubish it's state"""
        #create the corresponding class for the specified type
        types = Detector.DetectorTypes()
        typeClass = types.getClassForType(detector.type)
        confSection = types.getConfsectionForType(detector.type)
        #todo all Detectors are added as active; in case of a crash the PCA needs to remember which Detectors were active; maybe save this information in the ECS database?
        det = typeClass(detector.id,detector.address,detector.portTransition,detector.portCommand,confSection,self.log,self.publishQueue,self.handleDetectorTimeout,self.handleDetectorReconnect,self.dummyFunction,self.dummyFunction)
        self.detectors[det.id] = det
        self.publishQueue.put((det.id,det.getMappedState()))

        return True

    def removeDetector(self,id):
        """remove Detector from Dictionary"""
        if id not in self.detectors:
            self.log("Detector with id %s is unknown" % id,True)
            return False
        det = self.detectors[id]
        self.publishQueue.put((id,ECSCodes.removed))
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
        return det.abort()

    def isShutdown(self,detectorId):
        return self.detectors[detectorId].isShutdown()

    def shutdownDetector(self,detId):
        det = self.detectors[detId]
        if not det.powerOff():
            return False
        else:
            return True
