

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
import copy
import ECSCodes


class stateMapWrapper:
    """thread safe handling of Map"""
    semaphore = None
    statusMap = {}

    def __init__(self):
        self.semaphore = threading.Semaphore()

    def get(self,key):
        """get value for key returns None if key doesn't exist"""
        self.semaphore.acquire()
        if key in self.statusMap:
            ret = statusMap[key]
        else:
            ret = None
        self.semaphore.release()
        return ret

    def set(self,key,value):
        self.semaphore.acquire()
        self.statusMap[key] = value
        self.semaphore.release()

    def itemsCopy(self):
        """returns a deepcopy off all items for iteration"""
        #create copy of statusMap so loop dosn't crash if there are changes on statusMap during the loop
        #probably not the best solution, locks would be also not ideal
        self.semaphore.acquire()
        statusMap = copy.deepcopy(self.statusMap)
        self.semaphore.release()
        return statusMap.items()

    def isIn(self,key):
        """returns True if key exists in Map"""
        self.semaphore.acquire()
        ret = key in self.statusMap
        self.semaphore.release()
        return ret

class PCA:
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

    def __init__(self,logfile = "./log",debugMode = False):
        #init stuff
        self.stateMachine = Statemachine("PCAStatemachine.csv","NotReady")
        self.detectors = {}
        self.sequence = 0
        self.statusMap = stateMapWrapper()
        self.statusMap.set(0,(self.sequence,self.stateMachine.currentState))
        self.logfile = logfile

        #ZMQ Socket to publish new state Updates
        context = zmq.Context()
        self.socketPublish = context.socket(zmq.PUB)
        self.socketPublish.bind("tcp://*:5555")

        #publish logmessages
        self.socketLogPublish = context.socket(zmq.PUB)
        self.socketLogPublish.bind("tcp://*:5551")

        #Socket to wait for Updates From Detectors
        self.socketPullUpdates = context.socket(zmq.PULL)
        self.socketPullUpdates.bind("tcp://*:5556")

        #Socket to serve current statusMap
        self.socketServeCurrentStatus = context.socket(zmq.ROUTER)
        self.socketServeCurrentStatus.bind("tcp://*:5557")

        #socket for receiving Status Updates
        self.socketSingleRequest = context.socket(zmq.REP)
        self.socketSingleRequest.bind("tcp://*:%i" % 5553)

        #socket for receiving Status Updates
        self.remoteCommandSocket = context.socket(zmq.REP)
        self.remoteCommandSocket.bind("tcp://*:%i" % 5552)

        #register Poller
        self.poller = zmq.Poller()
        #self.poller.register(self.socketPullUpdates, zmq.POLLIN)
        self.poller.register(self.socketServeCurrentStatus, zmq.POLLIN)
        self.poller.register(self.socketSingleRequest, zmq.POLLIN)

        #init logger
        logging.basicConfig(
            format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
            #level = logging.DEBUG,
            handlers=[
            logging.FileHandler(logfile),
            logging.StreamHandler()
        ])
        logging.getLogger().setLevel(logging.INFO)
        logging.getLogger().handlers[0].setLevel(logging.INFO)
        if debugMode:
            logging.getLogger().handlers[1].setLevel(logging.INFO)
        else:
            logging.getLogger().handlers[1].setLevel(logging.CRITICAL)

        #thread stuff
        self.sem = threading.Semaphore()
        self.publishQueue = Queue()
        start_new_thread(self.waitForUpdates,())
        start_new_thread(self.waitForRequests,())
        start_new_thread(self.checkCurrentState,())
        start_new_thread(self.publisher,())
        start_new_thread(self.waitForRemoteCommand,())

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
            if m==b"ready":
                self.makeReady()
                continue
            if m==b"start":
                self.start()
                continue
            if m==b"stop":
                self.stop()
                continue
            if m==b"shutdown":
                self.shutdown()
                continue


    def publisher(self):
        """publishes all state changes inside the Queue"""
        while True:
            id,state = self.publishQueue.get()
            #self.publishStateUpdate(id,state)
            #race condition possible?
            self.sequence = self.sequence + 1
            #self.statusMap[id] = (self.sequence,state)
            self.statusMap.set(id,(self.sequence,state))
            self.send_status(self.socketPublish,id,self.sequence,state)


    def waitForUpdates(self):
        while True:
            message = self.socketPullUpdates.recv()
            message = message.decode()
            print (message)
            id = None
            command = None

            if len(message.split()) != 2:
                logging.critical("received empty or too long message",True)
                continue

            i,command = message.split()
            if i.isdigit():
                id = int(i)

            if id == None or command == None:
                logging.critical("received non-valid message",True)
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
                self.log("Detector "+str(det.id)+" made transition on it's own "+ oldstate +" -> " + det.stateMachine.currentState )
                self.publishQueue.put((det.id,det.getMappedState()))
            else:
                logging.critical("Detector"+ str(det.id) +" made an impossible Transition: " + command,True)
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
                    self.critical("wrong request in socketServeCurrentStatus \n")
                    continue

                #print (statusMap)
                # send each Statusmap entry to origin
                items = self.statusMap.itemsCopy()
                for key, value in items:
                    #send identity of origin first
                    self.socketServeCurrentStatus.send(origin,zmq.SNDMORE)
                    print (key,value[0],value[1])
                    self.send_status(self.socketServeCurrentStatus,key,value[0],value[1])
                    #self.socketServeCurrentStatus.send_multipart([key,status[0],status[1]])

                # Final message
                self.socketServeCurrentStatus.send(origin, zmq.SNDMORE)
                self.send_status(self.socketServeCurrentStatus,None,self.sequence,None)

            #request for single Detector
            if self.socketSingleRequest in items:
                message = self.socketSingleRequest.recv().decode()
                id = None
                if message.isdigit():
                    id = int(message)
                    if self.statusMap.isIn(id):
                        #self.socketSingleRequest.send(self.statusMap[id][1].encode())
                        self.socketSingleRequest.send(self.statusMap.get(id)[1].encode())
                    else:
                        self.socketSingleRequest.send(b"who?")
                        logging.critical("received status request for unknown id",True)
                else:
                    self.socketSingleRequest.send(b"numbers only")
                    self.log("received wrong message format in status request")


    def send_status(self,socket,key,sequence,state):
        """method for sending a status update on a specified socket"""
        #if None send empty byte String
        key_s=b""
        if key != None:
            #integers need to packed
            key_s = struct.pack("!i",key)

        sequence_s = struct.pack("!i",sequence)
        #python strings need to be encoded into binary strings
        state_b = b""
        if state != None:
            state_b = state.encode()
        socket.send_multipart([key_s,sequence_s,state_b])

    def addDetector(self,d):
        """add Detector to Dictionary and pubish it's state"""
        #semaphore is necessary otherwise threads iterating over map might crash
        self.sem.acquire()
        self.detectors[d.id] = d
        #self.statusMap[d.id] = d.getMappedState()
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
                        #print d.getMappedState()
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
            self.publishQueue.put((0,self.stateMachine.currentState))
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
        self.sem.acquire()
        for i,d in self.detectors.items():
            t = threading.Thread(name='dready'+str(i), target=self.threadFunctionCall, args=(d.id, d.getReady, returnQueue))
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
        self.sem.acquire()
        if self.stateMachine.currentState != "Ready":
            print ("start not possible in current state")
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

    test = PCA(debugMode=True)

    a = Detector.DetectorA(1,"DetectorStatemachine.csv","map.csv",5558,test.log)
    b = Detector.DetectorA(2,"DetectorStatemachine.csv","map.csv",5559,test.log)

    test.addDetector(a)
    test.addDetector(b)

    x = ""
    while x != "end":
        print ("1: get ready")
        print ("2: start")
        print ("3: stop")
        print ("4: shutdown")

        x = input()
        if x == "1":
            test.makeReady()
        if x == "2":
            test.start()
        if x== "3":
            test.stop()
        if x== "4":
            test.shutdown()
