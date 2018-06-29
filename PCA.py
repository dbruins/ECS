

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
    poller = None
    logfile = None

    sem = None
    publishQueue = None

    def __init__(self,logfile = "./log",debugMode = False):
        #init stuff
        self.stateMachine = Statemachine("PCAStatemachine.csv","NotReady")
        self.detectors = {}
        self.sequence = 0
        self.statusMap[0] = (self.sequence,self.stateMachine.currentState)
        self.logfile = logfile

        #ZMQ Socket to publish new state Updates
        context = zmq.Context()
        self.socketPublish = context.socket(zmq.PUB)
        self.socketPublish.bind("tcp://*:5555")

        #Socket to wait for Updates From Detectors
        self.socketPullUpdates = context.socket(zmq.PULL)
        self.socketPullUpdates.bind("tcp://*:5556")

        #Socket to serve current statusMap
        self.socketServeCurrentStatus = context.socket(zmq.ROUTER)
        self.socketServeCurrentStatus.bind("tcp://*:5557")

        #socket for receiving Status Updates
        self.socketSingleRequest = context.socket(zmq.REP)
        self.socketSingleRequest.bind("tcp://*:%i" % 5553)

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
        while True:
            message = self.socketPullUpdates.recv()
            message = message.decode()
            print (message)
            id = None
            command = None

            if len(message.split()) != 2:
                logging.critical("received empty or too long message")
                continue

            i,command = message.split()
            if i.isdigit():
                id = int(i)

            if id == None or command == None:
                logging.critical("received non-valid message")
                continue
            if id not in self.detectors:
                logging.critical("received message with unknown id")
                continue

            det = self.detectors[id]
            self.sem.acquire()
            nextMappedState = det.getMappedStateForCommand(command)
            #Detector may not start Running on it's own
            if nextMappedState and nextMappedState != "Running":
                oldstate = det.stateMachine.currentState
                det.stateMachine.transition(command)
                logging.info("Detector "+str(det.id)+" made transition on it's own "+ oldstate +" -> " + det.stateMachine.currentState )
                self.publishQueue.put((det.id,det.getMappedState()))
            else:
                logging.critical("Detector"+ str(det.id) +" made an impossible Transition: " + command)
                #todo Ok what now?
            self.sem.release()

    def waitForRequests(self):
        """waits for messages from detectors"""
        while True:
            try:
                items = dict(self.poller.poll())
            except:
                logging.critical("unexpected error while receiving messages")
                break

            #request for entire statusMap e.g. if a new Detector/Client connects
            if self.socketServeCurrentStatus in items:
                messsage = self.socketServeCurrentStatus.recv_multipart()
                origin = messsage[0]
                request = messsage[1]
                if request != b"HI":
                    self.critical("wrong request in socketServeCurrentStatus \n")
                    continue

                # Create Route from self to requester
                #route = Route(socketServeCurrentStatus, origin)

                # send each Statusmap entry to origin
                for key, value in self.statusMap.items():
                    #send identity of origin first
                    self.socketServeCurrentStatus.send(origin,zmq.SNDMORE)
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
                    if id in self.statusMap:
                        self.socketSingleRequest.send(self.statusMap[id][1].encode())
                    else:
                        self.socketSingleRequest.send(b"who?")
                        logging.critical("received status request for unknown id")
                else:
                    self.socketSingleRequest.send(b"numbers only")
                    logging.critical("received wrong message format in status request")


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
        """add Detector to Dictionary"""
        #todo causes problems when some other Thread is iterating over detectors
        self.detectors[d.id] = d
        self.statusMap[d.id] = d.getMappedState()
        self.publishQueue.put((d.id,d.getMappedState()))

    def removeDetector(self,id):
        """remove Detector from Dictionary"""
        #todo muss irgendwie richtig mitgeteilt werden
        self.publishQueue.put((d.id,"shutdown"))
        del self.detectors[id]


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
            logging.info("GLobal Statechange: "+oldstate+" -> "+self.stateMachine.currentState)

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
                logging.info("error shuting down Detector "+str(d.id))

    def makeReady(self):
        """tells all Detectors to get ready to start"""
        threadArray = []
        returnQueue = Queue()
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
                logging.info("error getting ready from Detector "+str(d.id))

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
                logging.info("error while starting from Detector "+str(d.id))
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
                logging.info("error while stopping from Detector "+str(d.id))
        self.transition("stop")
        self.sem.release()

    def log(self,logmessage,error=False):
        with open(self.logfile,"a") as log:
            if error:
                print(logmessage)
                log.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+": Error: "+logmessage+"\n")
            else:
                print(logmessage)
                log.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S")+": "+logmessage+"\n")




if __name__ == "__main__":

    test = PCA(debugMode=True)

    a = Detector.DetectorA(1,"DetectorStatemachine.csv","map.csv",5558)
    b = Detector.DetectorA(2,"DetectorStatemachine.csv","map.csv",5559)

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
