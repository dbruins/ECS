#!/usr/bin/python3

from Statemachine import Statemachine
import csv
from _thread import start_new_thread
from multiprocessing import Queue
import zmq
import Detector
from datetime import datetime
import threading
import struct # for packing integers
import logging
import ECSCodes
import configparser
import copy
import json
from DataObjects import DataObjectCollection, detectorDataObject, partitionDataObject
import sys

class MapWrapper:
    """thread safe handling of Map"""
    def __init__(self):
        self.map = {}
        self.semaphore = threading.Semaphore()

    def __iter__(self):
        return self.copy().values().__iter__()

    def __getitem__(self, key):
        """get value for key returns None if key doesn't exist"""
        self.semaphore.acquire()
        if key in self.map:
            ret = self.map[key]
        else:
            ret = None
        self.semaphore.release()
        return ret

    def __delitem__(self,key):
        self.semaphore.acquire()
        if key in self.map:
            del self.map[key]
        self.semaphore.release()


    def __setitem__(self,key,value):
        self.semaphore.acquire()
        self.map[key] = value
        self.semaphore.release()

    def copy(self):
        """returns a deepcopy off all items for iteration"""
        #create copy of statusMap so loop dosn't crash if there are changes on statusMap during the loop
        #probably not the best solution
        self.semaphore.acquire()
        mapCopy = copy.deepcopy(self.map)
        self.semaphore.release()
        return mapCopy

    def __contains__(self, key):
        self.semaphore.acquire()
        ret = key in self.map
        self.semaphore.release()
        return ret

    def size(self):
        self.semaphore.acquire()
        r = len(self.map)
        self.semaphore.release()
        return r

    def items(self):
        return self.copy().items()

class PCA:
    id = None
    detectors = None
    stateMachine = None
    #id -> (sequence, status)
    statusMap = {}
    sequence = 0

    #---zmq sockets---
    #publish state updates
    socketPublish = None
    #pull updates from Detectors
    socketPullUpdates = None
    #update request from new Detectors or Detecotors with previous connection Problem
    socketServeCurrentStatus = None
    #request of status of an id
    socketSingleRequest = None
    #publish logmessages
    socketLogPublish = None
    poller = None

    logfile = None

    sem = None
    publishQueue = None
    commandQueue = None

    def __init__(self,id):
        self.id = id
        #read config File
        config = configparser.ConfigParser()
        config.read("init.cfg")
        conf = config["Default"]
        context = zmq.Context()

        #get your config
        configECS = None
        while configECS == None:
            requestSocket = context.socket(zmq.REQ)
            requestSocket.connect("tcp://%s:%s" % (conf['ECSAddress'],conf['ECSRequestPort']))
            requestSocket.setsockopt(zmq.RCVTIMEO, int(conf['receive_timeout']))
            requestSocket.setsockopt(zmq.LINGER,0)

            requestSocket.send_multipart([ECSCodes.pcaAsksForConfig, id.encode()])
            try:
                configJSON = requestSocket.recv().decode()
                configJSON = json.loads(configJSON)
                configECS = partitionDataObject(configJSON)
            except zmq.Again:
                print("timeout getting configuration")
                requestSocket.close()
                continue
            requestSocket.close()

        #init stuff
        self.stateMachine = Statemachine(conf["stateMachineCSV"],conf["initialState"])
        self.detectors = {}
        self.activeConnectors = MapWrapper()
        self.sequence = 0
        #the only one who may change this, is the publisher thread
        self.statusMap = MapWrapper()
        self.sem = threading.Semaphore()
        self.publishQueue = Queue()
        self.commandQueue = Queue()

        ports = config["ZMQPorts"]
        #ZMQ Socket to publish new state Updates
        self.socketPublish = context.socket(zmq.PUB)
        self.socketPublish.bind("tcp://*:%s" % configECS.portPublish)

        #publish logmessages
        self.socketLogPublish = context.socket(zmq.PUB)
        self.socketLogPublish.bind("tcp://*:%s" % configECS.portLog)

        #Socket to wait for Updates From Detectors
        self.socketPullUpdates = context.socket(zmq.PULL)
        self.socketPullUpdates.bind("tcp://*:%s" % configECS.portUpdates)

        #Socket to serve current statusMap
        self.socketServeCurrentStatus = context.socket(zmq.ROUTER)
        self.socketServeCurrentStatus.bind("tcp://*:%s" % configECS.portCurrentState)

        #socket for receiving Status Updates
        self.socketSingleRequest = context.socket(zmq.REP)
        self.socketSingleRequest.bind("tcp://*:%s" % configECS.portSingleRequest)

        #socket for receiving commands
        self.remoteCommandSocket = context.socket(zmq.REP)
        self.remoteCommandSocket.bind("tcp://*:%s" % configECS.portCommand)

        #register Poller
        self.poller = zmq.Poller()
        #self.poller.register(self.socketPullUpdates, zmq.POLLIN)
        self.poller.register(self.socketServeCurrentStatus, zmq.POLLIN)
        self.poller.register(self.socketSingleRequest, zmq.POLLIN)

        #init logger
        self.logfile = conf["logPath"]
        debugMode = bool(conf["debugMode"])
        logging.basicConfig(
            format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
            #level = logging.DEBUG,
            handlers=[
            logging.FileHandler(self.logfile),
            logging.StreamHandler()
        ])
        logging.getLogger().setLevel(logging.INFO)
        logging.getLogger().handlers[0].setLevel(logging.INFO)
        #set console log to info level if in debug mode
        if debugMode:
            logging.getLogger().handlers[1].setLevel(logging.INFO)
        else:
            logging.getLogger().handlers[1].setLevel(logging.CRITICAL)

        #get your Detectorlist
        detList = None
        while detList == None:
            requestSocket = context.socket(zmq.REQ)
            requestSocket.connect("tcp://%s:%s" % (conf['ECSAddress'],conf['ECSRequestPort']))
            requestSocket.setsockopt(zmq.RCVTIMEO, int(conf['receive_timeout']))
            requestSocket.setsockopt(zmq.LINGER,0)

            requestSocket.send_multipart([ECSCodes.pcaAsksForDetectorList, self.id.encode()])
            try:
                #receive detectors as json
                detJSON = requestSocket.recv().decode()
                detJSON = json.loads(detJSON)
                #create DataObjectCollection from JSON
                detList = DataObjectCollection(detJSON,detectorDataObject)
            except zmq.Again:
                self.log("timeout getting DetectorList", True)
                requestSocket.close()
                continue
            requestSocket.close()

        #create Detector objects
        for d in detList:
            id = d.id
            address = d.address
            type = d.type
            port = d.port

            #create the corresponding class for the specified type
            types = Detector.DetectorTypes()
            typeClass = types.getClassForType(type)
            det = typeClass(id,address,port,self.log,self.activeConnectors,self.publishQueue)
            self.addDetector(det)

        #thread stuff
        start_new_thread(self.waitForUpdates,())
        start_new_thread(self.waitForRequests,())
        start_new_thread(self.checkCurrentState,())
        start_new_thread(self.publisher,())
        start_new_thread(self.waitForRemoteCommand,())
        start_new_thread(self.watchCommandQueue,())

        #set and publish PCA Globalstate
        self.statusMap[self.id] = (self.sequence,self.stateMachine.currentState)
        self.publishQueue.put((self.id,self.stateMachine.currentState))

    def watchCommandQueue(self):
        """watch the command Queue und execute incoming commands"""
        while True:
            command = self.commandQueue.get()
            if command==b"ready":
                self.makeReady()
                continue
            if command==b"start":
                self.start()
                continue
            if command==b"stop":
                self.stop()
                continue
            if command==b"shutdown":
                self.shutdown()
                continue

    def waitForRemoteCommand(self):
        """wait for an external(start,stop etc.) command e.g. from that "gorgeous" WebGUI
        sends an Ok for received command"""
        while True:
            m = self.remoteCommandSocket.recv()
            if m!=ECSCodes.ping:
                print (m)
            self.remoteCommandSocket.send(ECSCodes.ok)
            if m==ECSCodes.ping:
                #it's just a ping
                continue
            self.commandQueue.put(m)


    def publisher(self):
        """publishes all state changes inside the Queue"""
        while True:
            id,state = self.publishQueue.get()
            #self.publishStateUpdate(id,state)
            #race condition possible?
            self.sequence = self.sequence + 1
            self.statusMap[id] = (self.sequence,state)
            self.send_status(self.socketPublish,id,self.sequence,state)


    def waitForUpdates(self):
        """wait for updates from Detectors"""
        while True:
            message = self.socketPullUpdates.recv()
            message = message.decode()
            id = None
            command = None

            if len(message.split()) != 2:
                self.log("received empty or too long message",True)
                continue

            id,command = message.split()

            if id == None or command == None:
                self.log("received non-valid message",True)
                continue
            if id not in self.detectors:
                self.log("received message with unknown id",True)
                continue

            det = self.detectors[id]
            self.sem.acquire()
            nextMappedState = det.getMappedStateForCommand(command)
            #Detector may not start Running on it's own
            if nextMappedState and nextMappedState != "Running":
                oldstate = det.stateMachine.currentState
                det.stateMachine.transition(command)
                self.log("Detector "+det.id+" made transition on it's own "+ oldstate +" -> " + det.stateMachine.currentState )
                self.publishQueue.put((det.id,det.getMappedState()))
            else:
                self.log("Detector"+ det.id +" made an impossible Transition: " + str(command),True)
                #todo Ok what now?
            self.sem.release()

    def waitForRequests(self):
        """waits for messages from detectors"""
        while True:
            try:
                items = dict(self.poller.poll())
            except:
                self.log("unexpected error while receiving messages")
                break

            #request for entire statusMap e.g. if a new Detector/Client connects
            if self.socketServeCurrentStatus in items:
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
                    self.send_status(self.socketServeCurrentStatus,key,value[0],value[1])
                    #self.socketServeCurrentStatus.send_multipart([key,status[0],status[1]])

                # Final message
                self.socketServeCurrentStatus.send(origin, zmq.SNDMORE)
                self.send_status(self.socketServeCurrentStatus,None,self.sequence,None)

            #request for single Detector
            if self.socketSingleRequest in items:
                id = self.socketSingleRequest.recv().decode()

                if id in self.statusMap:
                    #send status
                    self.socketSingleRequest.send(self.statusMap.get[id][1].encode())
                else:
                    self.socketSingleRequest.send(ECSCodes.idUnknown)
                    self.log("received status request for unknown id",True)


    def send_status(self,socket,key,sequence,state):
        """method for sending a status update on a specified socket"""
        #if None send empty byte String
        key_b=b""
        if key != None:
            key_b = key.encode()

        #integers need to be packed
        sequence_s = struct.pack("!i",sequence)
        #python strings need to be encoded into binary strings
        state_b = b""
        if state != None:
            state_b = state.encode()
        socket.send_multipart([key_b,sequence_s,state_b])

    def addDetector(self,d):
        """add Detector to Dictionary and pubish it's state"""
        #d = Detector.DetectorA(id,configSection,self.log)
        #semaphore is necessary otherwise threads iterating over map might crash
        self.sem.acquire()
        self.detectors[d.id] = d
        self.publishQueue.put((d.id,d.getMappedState()))
        self.sem.release()

    def removeDetector(self,id):
        """remove Detector from Dictionary"""
        #todo muss irgendwie richtig mitgeteilt werden
        self.sem.acquire()
        self.publishQueue.put((d.id,"shutdown"))
        del self.detectors[id]
        self.sem.release()


    def checkCurrentState(self):
        """checks in a loop if the current state is still valid, does necessary transition in case it is not"""
        while True:
            if len(self.detectors.items()) <= 0:
                continue
            #don't check your state while it's beeing changed that might induce chaos
            self.sem.acquire()
            if self.stateMachine.currentState == "NotReady":
                ready = True
                for i,d in self.detectors.items():
                    if d.getMappedState() != "Ready":
                        ready = False
                if ready:
                    self.transition("configured")

            if self.stateMachine.currentState == "Ready":
                for i,d in self.detectors.items():
            		#The PCA is not allowed to move into the Running State on his own
            		#only ready -> not ready is possible
                    if d.getMappedState() != "Ready":
                        # some Detectors are not ready anymore
                        self.error()


            if self.stateMachine.currentState == "Running":
                for i,d in self.detectors.items():
                #Some Detector stopped Working
                    if d.getMappedState() != "Running":
                        self.error()

            if self.stateMachine.currentState == "RunningInError":
                countDetectors = 0
                for i,d in self.detectors.items():
                    if d.getMappedState() == "Running":
                        countDetectors = countDetectors +1
                if countDetectors == len(self.detectors):
                    #All Detecotors are working again
                    self.transition("resolved")
                if countDetectors == 0:
                    #All detectors are dead :(
                    self.transition("stop")
            self.sem.release()

    def transition(self,command):
        """try to transition the own Statemachine"""
        oldstate = self.stateMachine.currentState
        if self.stateMachine.transition(command):
            self.publishQueue.put((self.id,self.stateMachine.currentState))
            self.log("GLobal Statechange: "+oldstate+" -> "+self.stateMachine.currentState)

    def error(self):
        """make an error transition"""
        self.transition("error")

    def threadFunctionCall(self,id,function,retq):
        """calls a function from Detector(id) und puts result in a given Queue"""
        r = function()
        retq.put((id,r))

    def shutdown(self):
        """tells all Detectors to shutdown"""
        threadArray = []
        returnQueue = Queue()
        if not (len(self.detectors)==self.activeConnectors.size()):
            self.log("Warning: Some Detectors are disconnected")
        self.sem.acquire()
        for i,d in self.detectors.items():
            t = threading.Thread(name='doff'+str(i), target=self.threadFunctionCall, args=(d.id, d.powerOff, returnQueue))
            threadArray.append(t)
            t.start()
        for t in threadArray:
            ret = returnQueue.get()
            d = self.detectors[ret[0]]
            if ret[1] == True:
                self.publishQueue.put((d.id,d.getMappedState()))
            else:
                #todo something needs to happen here
                self.log("error shuting down Detector "+str(d.id),True)
        self.sem.release()

    def makeReady(self):
        """tells all Detectors to get ready to start"""
        threadArray = []
        returnQueue = Queue()
        if not (len(self.detectors)==self.activeConnectors.size()):
            self.log("Warning: Some Detectors are disconnected")
        self.sem.acquire()
        for id in self.activeConnectors:
            d = self.detectors[id]
            t = threading.Thread(name='dready'+str(id), target=self.threadFunctionCall, args=(d.id, d.getReady, returnQueue))
            threadArray.append(t)
            t.start()

        for t in threadArray:
            ret = returnQueue.get()
            d = self.detectors[ret[0]]
            if ret[1] == True:
                self.publishQueue.put((d.id,d.getMappedState()))
            else:
                #todo something needs to happen here
                self.log("error getting ready from Detector "+str(d.id),True)
        self.sem.release()
    def start(self):
        """tells all Detectors to start running"""
        if not (len(self.detectors)==self.activeConnectors.size()):
            self.log("Warning: Some Detectors are disconnected")
        self.sem.acquire()
        if self.stateMachine.currentState != "Ready":
            self.log("start not possible in current state")
            self.sem.release()
            return
        threadArray = []
        returnQueue = Queue()
        for i,d in self.detectors.items():
            t = threading.Thread(name='dstart'+str(i), target=self.threadFunctionCall, args=(d.id, d.start, returnQueue))
            threadArray.append(t)
            t.start()

        for t in threadArray:
            ret = returnQueue.get()
            d = self.detectors[ret[0]]
            if ret[1] == True:
                self.publishQueue.put((d.id,d.getMappedState()))
            else:
                #todo something needs to happen here
                self.log("error while starting from Detector "+str(d.id),True)
        self.transition("start")
        self.sem.release()

    def stop(self):
        """tells all Detectors to stop running"""
        threadArray = []
        returnQueue = Queue()
        if not (len(self.detectors)==self.activeConnectors.size()):
            self.log("Warning: Some Detectors are disconnected")
        self.sem.acquire()
        for i,d in self.detectors.items():
            t = threading.Thread(name='dstop'+str(i), target=self.threadFunctionCall, args=(d.id, d.stop, returnQueue))
            threadArray.append(t)
            t.start()

        for t in threadArray:
            ret = returnQueue.get()
            d = self.detectors[ret[0]]
            if ret[1] == True:
                self.publishQueue.put((d.id,d.getMappedState()))
            else:
                #todo something needs to happen here
                self.log("error while stopping from Detector "+str(d.id),True)
        self.transition("stop")
        self.sem.release()

    def log(self,logmessage,error=False):
        str=datetime.now().strftime("%Y-%m-%d %H:%M:%S")+":" + logmessage
        self.socketLogPublish.send(str.encode())
        if error:
            logging.critical(logmessage)
        else:
            logging.info(logmessage)




if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("please enter the pca id")
        sys.exit(1)

    test = PCA(sys.argv[1])

    x = ""
    while x != "end":
        print ("1: get ready")
        print ("2: start")
        print ("3: stop")
        print ("4: shutdown")

        x = input()
        if x == "1":
            test.commandQueue.put(b"ready")
        if x == "2":
            test.commandQueue.put(b"start")
        if x== "3":
            test.commandQueue.put(b"stop")
        if x== "4":
            test.commandQueue.put(b"shutdown")
