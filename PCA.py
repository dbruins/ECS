#!/usr/bin/python3

from Statemachine import Statemachine
import csv
from _thread import start_new_thread
from multiprocessing import Queue
import zmq
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
import Detector
from ECS_tools import MapWrapper
import ECS_tools
import time
import zc.lockfile #pip3 install zc.lockfile
import signal

class PCA:
    def __init__(self,id):
        self.id = id

        #create lock
        try:
            self.lock = zc.lockfile.LockFile('lock'+self.id, content_template='{pid}')
        except zc.lockfile.LockError:
            print("other Process is already Running "+self.id)
            exit(1)
        self.terminate = threading.Event()
        self.initDone = threading.Event()
        #handle SIGTERM signal
        signal.signal(signal.SIGTERM, self.terminatePCA)

        #read config File
        config = configparser.ConfigParser()
        config.read("init.cfg")
        conf = config["Default"]
        self.context = zmq.Context()
        self.receive_timeout = int(conf['receive_timeout'])
        self.ECSAdress = conf['ECSAddress']
        self.ECSRequestPort = conf['ECSRequestPort']

        #get your config
        configECS = None
        while configECS == None:
            requestSocket = self.context.socket(zmq.REQ)
            requestSocket.connect("tcp://%s:%s" % (conf['ECSAddress'],conf['ECSRequestPort']))
            requestSocket.setsockopt(zmq.RCVTIMEO, int(conf['receive_timeout']))
            requestSocket.setsockopt(zmq.LINGER,0)

            requestSocket.send_multipart([ECSCodes.pcaAsksForConfig, id.encode()])
            try:
                #returns None in pca is unknown
                configJSON = requestSocket.recv()
                if configJSON == ECSCodes.idUnknown:
                    print("id %s is not in Database" % self.id)
                    sys.exit(1)
                configJSON = json.loads(configJSON.decode())
                configECS = partitionDataObject(configJSON)
            except zmq.Again:
                print("timeout getting configuration")
                continue
            except zmq.error.ContextTerminated:
                pass
            finally:
                requestSocket.close()

        #init stuff
        self.detectors = MapWrapper()
        self.activeDetectors = MapWrapper()
        #update number
        self.sequence = 0
        self.transitionNumber = 0
        self.pendingTransitions = MapWrapper()
        #the only one who may change the status Map, is the publisher thread
        self.statusMap = MapWrapper()
        self.sem = threading.Semaphore()
        self.publishQueue = Queue()
        self.commandQueue = Queue()

        #ZMQ Socket to publish new state Updates
        self.socketPublish = self.context.socket(zmq.PUB)
        #todo the connect takes a little time messages until then will be lost
        self.socketPublish.bind("tcp://*:%s" % configECS.portPublish)

        #publish logmessages
        self.socketLogPublish = self.context.socket(zmq.PUB)
        self.socketLogPublish.bind("tcp://*:%s" % configECS.portLog)

        #Socket to wait for Updates From Detectors
        self.socketDetectorUpdates = self.context.socket(zmq.REP)
        self.socketDetectorUpdates.bind("tcp://*:%s" % configECS.portUpdates)

        #Socket to serve current statusMap
        self.socketServeCurrentStatus = self.context.socket(zmq.ROUTER)
        self.socketServeCurrentStatus.bind("tcp://*:%s" % configECS.portCurrentState)

        #socket for receiving commands
        self.remoteCommandSocket = self.context.socket(zmq.REP)
        self.remoteCommandSocket.bind("tcp://*:%s" % configECS.portCommand)

        #init logger
        self.logfile = conf["logPath"]
        debugMode = bool(conf["debugMode"])
        logging.basicConfig(
            format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
            #level = logging.DEBUG,
            handlers=[
            #logging to file
            logging.FileHandler(self.logfile),
            #logging on console and WebUI
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
            requestSocket = self.context.socket(zmq.REQ)
            requestSocket.connect("tcp://%s:%s" % (conf['ECSAddress'],conf['ECSRequestPort']))
            requestSocket.setsockopt(zmq.RCVTIMEO, int(conf['receive_timeout']))
            requestSocket.setsockopt(zmq.LINGER,0)

            requestSocket.send_multipart([ECSCodes.pcaAsksForDetectorList, self.id.encode()])
            try:
                #receive detectors as json
                ret = requestSocket.recv()
                if ret == ECSCodes.error:
                    self.log("received error getting DetectorList", True)
                    continue
                detJSON = json.loads(ret.decode())
                #create DataObjectCollection from JSON
                detList = DataObjectCollection(detJSON,detectorDataObject)
            except zmq.Again:
                self.log("timeout getting DetectorList", True)
                continue
            except zmq.error.ContextTerminated:
                pass
            finally:
                requestSocket.close()

        start_new_thread(self.publisher,())

        #Subscribers need some time to subscribe (todo there has to be a better way)
        time.sleep(1)
        #tell subscribers to reset their state Table
        self.publishQueue.put((self.id,ECSCodes.reset))
        #will be set after all Detectors are added
        self.stateMachine = None

        #create Detector objects
        threadArray = []
        for d in detList:
            #in case there of a connection problem, creating a Detector might take a long time, therefore create a own thread for each detector
            t = threading.Thread(name='dcreate'+str(d.id), target=self.addDetector, args=(d,))
            threadArray.append(t)
            t.start()
        for t in threadArray:
            t.join()

        #set and publish PCA Globalstate
        #calculate global state
        globalState = "NotReady"
        allReady = True
        allRunning = True
        running = False
        for id in self.activeDetectors:
            d = self.detectors[id]
            if d.getMappedState() == "Running" and not running:
                running = True
            if d.getMappedState() != "Running":
                allRunning = False
            if d.getMappedState() != "Ready" and not running:
                allReady = False

        if running and not allRunning:
            globalState = "RunningInError"
        if allRunning:
            globalState = "Running"
        if not running and allReady:
            globalState = "Ready"

        self.stateMachine = Statemachine(conf["stateMachineCSV"],globalState)

        self.publishQueue.put((self.id,self.stateMachine.currentState))
        self.checkGlobalState()

        #thread stuff
        start_new_thread(self.waitForUpdates,())
        start_new_thread(self.waitForRequests,())
        start_new_thread(self.waitForRemoteCommand,())
        start_new_thread(self.watchCommandQueue,())
        self.initDone.set()

    def watchCommandQueue(self):
        """watch the command Queue und execute incoming commands"""
        while True:
            m = self.commandQueue.get()
            if self.terminate.is_set():
                break
            command,arg = m
            if command==ECSCodes.getReady:
                self.makeReady()
                continue
            if command==ECSCodes.start:
                self.start()
                continue
            if command==ECSCodes.stop:
                self.stop()
                continue
            if command==ECSCodes.shutdown:
                self.shutdown()
                continue
            if command == ECSCodes.setActive:
                self.setDetectorActive(arg)
                continue
            if command == ECSCodes.setInactive:
                self.setDetectorInactive(arg)
                continue
            if command == ECSCodes.removeDetector:
                self.removeDetector(arg)
                continue
            if command == ECSCodes.addDetector:
                detector = detectorDataObject(json.loads(arg))
                self.addDetector(detector)
                continue
            if command == ECSCodes.abort:
                self.abort()
                continue

    def waitForRemoteCommand(self):
        """wait for an external(start,stop etc.) command e.g. from that "gorgeous" WebGUI
        sends an Ok for received command"""
        while True:
            try:
                command = self.remoteCommandSocket.recv_multipart()
                arg = None
                if len(command) > 1:
                    arg = command[1].decode()
                command = command[0]

                if command==ECSCodes.ping:
                    self.remoteCommandSocket.send(ECSCodes.ok)
                    #it's just a ping
                    continue

                def checkConsistencyRequest(detectorList):
                    detectorList = DataObjectCollection(json.loads(detectorList),detectorDataObject)
                    start_new_thread(self.checkSystemConsistency,(detectorList,))
                    return ECSCodes.ok

                def switcher(code,arg=None):
                    #functions for codes
                    dbFunctionDictionary = {
                        ECSCodes.getReady: self.makeReady,
                        ECSCodes.start: self.start,
                        ECSCodes.stop: self.stop,
                        ECSCodes.shutdown: self.shutdown,
                        ECSCodes.setActive: self.setDetectorActive,
                        ECSCodes.setInactive: self.setDetectorInactive,
                        ECSCodes.removeDetector: self.removeDetector,
                        ECSCodes.addDetector: self.addDetector,
                        ECSCodes.abort: self.abort,
                        ECSCodes.check: checkConsistencyRequest,
                    }
                    #returns function for Code or None if the received code is unknown
                    f = dbFunctionDictionary.get(code,None)
                    if not f:
                        self.remoteCommandSocket.send(ECSCodes.unknownCommand)
                        return
                    if arg:
                        ret = f(arg)
                    else:
                        ret = f()
                    self.remoteCommandSocket.send(ret)
                if arg:
                    switcher(command,arg)
                else:
                    switcher(command)
            except zmq.error.ContextTerminated:
                self.remoteCommandSocket.close()
                break



    def publisher(self):
        """publishes all state changes inside the Queue"""
        while True:
            id,state = self.publishQueue.get()
            if self.terminate.is_set():
                break
            self.sequence = self.sequence + 1
            if state == ECSCodes.removed:
                del self.statusMap[id]
            else:
                self.statusMap[id] = (self.sequence,state)
            ECS_tools.send_status(self.socketPublish,id,self.sequence,state)


    def waitForUpdates(self):
        """wait for updates from Detectors"""
        while True:
            try:
                message = self.socketDetectorUpdates.recv_multipart()
                print (message)
                if len(message) != 3:
                    self.log("received too short or too long message: %s" % message,True)
                    continue
                id, transitionNumber, state = message
                id = id.decode()
                transitionNumber = ECS_tools.intFromBytes(transitionNumber)
                state = state.decode()

                if id not in self.detectors:
                    self.log("received message with unknown id: %s" % id,True)
                    self.socketDetectorUpdates.send(ECSCodes.idUnknown)
                    continue
                self.socketDetectorUpdates.send(ECSCodes.ok)

                det = self.detectors[id]
                if (det.stateMachine.currentState == state):
                    continue
                oldstate = det.stateMachine.currentState
                det.stateMachine.currentState = state

                print(self.pendingTransitions)
                if id in self.pendingTransitions:
                    if self.pendingTransitions[id] == transitionNumber:
                        self.log("Detector %s Transition %i done %s -> %s" % (det.id,transitionNumber,oldstate,det.stateMachine.currentState))
                        det.inTransition = False
                    else:
                        self.log("Detector "+det.id+" send wrong Transition Number %i (expected %i) Transition: %s -> %s" % (transitionNumber,self.pendingTransitions[id],oldstate,det.stateMachine.currentState),True )
                        #todo what to do?
                        det.inTransition = False
                    del self.pendingTransitions[id]
                else:
                    #if Detector was never connected oldstate will be False (no log necessary)
                    if oldstate:
                        self.log("Detector "+det.id+" had an unexpected Statechange "+ str(oldstate) +" -> " + det.stateMachine.currentState )
                    self.transitionDetectorIntoGlobalState(det.id)
                self.publishQueue.put((det.id,det.getMappedState()))
                self.checkGlobalState()
            except zmq.error.ContextTerminated:
                self.socketDetectorUpdates.close()
                break

    def waitForRequests(self):
        """waits for messages from detectors/ECS/WebServer"""
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

    def checkSystemConsistency(self,detList):
        """check wether the Detector List and States are correct"""
        #check detectorList
        for detId in self.detectors.keyIterator():
            if detId not in detList.asDictionary():
                self.log("System check: Detector %s should not have been a Part of Partition %s" % (detId,self.id),True)
                self.removeDetector(detId)
        for d in detList:
            if d.id not in self.detectors:
                self.log("System check: Detector %s was not in Partition %s" % (d.id,self.id),True)
                self.addDetector(d)

        #check DetectorStates
        for detId in self.detectors.keyIterator():
            if detId in self.detectors:
                d = self.detectors[detId]
                state = d.getStateFromDetector()
                if not state:
                    #timeout getting State let the pingThread handele it
                    continue
                if d.stateMachine.currentState != state:
                    self.log("During System check Detector %s returned an unexpected state: %s" % (d.id,state),True)
                    d.stateMachine.currentState = state

    def addDetector(self,detector):
        """add Detector to Dictionary and pubish it's state"""
        self.sem.acquire()
        try:
            if isinstance(detector,str):
                detector = detectorDataObject(json.loads(detector))
            #create the corresponding class for the specified type
            types = Detector.DetectorTypes()
            typeClass = types.getClassForType(detector.type)
            if not typeClass:
                return False
            confSection = types.getConfsectionForType(detector.type)
            det = typeClass(detector.id,detector.address,detector.portTransition,detector.portCommand,confSection,self.log,self.publishQueue,self.handleDetectorTimeout,self.handleDetectorReconnect,self.putPendingTransition,self.removePendingTransition)
            self.detectors[det.id] = det
            if det.active:
                self.activeDetectors[det.id] = det.id
            self.publishQueue.put((det.id,det.getMappedState()))
            #global state doesn't exist during start up
            if self.stateMachine:
                start_new_thread(self.transitionDetectorIntoGlobalState,(det.id,))
                self.checkGlobalState()
        except Exception as e:
            self.log("Exception while adding Detector %s: %s" %(detector.id,str(e)))
        finally:
            self.sem.release()
        return ECSCodes.ok

    def removeDetector(self,id):
        """remove Detector from Dictionary"""
        self.sem.acquire()
        try:
            if id not in self.detectors:
                self.log("Detector with id %s is unknown" % id,True)
                return ECSCodes.idUnknown
            det = self.detectors[id]
            #todo add possibility to cancel transitions?
            self.publishQueue.put((id,ECSCodes.removed))
            self.checkGlobalState()
            del self.detectors[id]
            del self.activeDetectors[id]
            self.removePendingTransition(id)
            #this might take a few seconds dending on ping interval
            start_new_thread(det.terminate,())
        except Exception as e:
            self.log("Exception while removing Detector %s: %s" %(detector.id,str(e)))
        finally:
            self.sem.release()
        return ECSCodes.ok

    def setDetectorActive(self,id):
        """set detector active"""
        if id not in self.detectors:
            self.log("Detector with id %s is unknown" % id,True)
            return ECSCodes.idUnknown
        detector = self.detectors[id]
        detector.setActive()
        self.activeDetectors[id] = id
        self.publishQueue.put((id,detector.getMappedState()))
        self.checkGlobalState()
        start_new_thread(self.transitionDetectorIntoGlobalState,(id,))
        return ECSCodes.ok


    def setDetectorInactive(self,id):
        """set Detector inactive"""
        if id not in self.detectors:
            self.log("Detector with id %s is unknown" % id,True)
            return ECSCodes.idUnknown
        detector = self.detectors[id]
        del self.activeDetectors[id]
        #detector.powerOff()
        detector.setInactive()
        self.publishQueue.put((id,"inactive"))
        self.checkGlobalState()
        return ECSCodes.ok

    def handleDetectorTimeout(self,id):
        #wait until Statemachine initialised
        self.initDone.wait()
        self.publishQueue.put((id,"Connection Problem"))
        self.checkGlobalState()

    def handleDetectorReconnect(self,id,state):
        #wait until Statemachine initialised
        self.initDone.wait()
        self.publishQueue.put((id,state))
        #if detector is active try to transition him into globalState
        if id in self.activeDetectors:
            self.transitionDetectorIntoGlobalState(id)
            self.checkGlobalState()

    def transitionDetectorIntoGlobalState(self,id):
        """try to transition Detector to global State of the PCA"""
        det = self.detectors[id]
        if self.stateMachine.currentState == "NotReady":
            if det.getMappedState() == "Running":
                det.stop()
                return
        if self.stateMachine.currentState == "Ready":
            if det.getMappedState() == "NotReady":
                det.getReady()
                return
            if det.getMappedState() == "Running":
                det.stop()
                return
        if self.stateMachine.currentState == "Running" or self.stateMachine.currentState == "RunningInError":
            if det.getMappedState() == "NotReady":
                det.getReady()
                det.start()
                return
            if det.getMappedState() == "Ready":
                det.start()
                return

    def checkGlobalState(self):
        if self.stateMachine.currentState == "NotReady":
            ready = True
            for id in self.activeDetectors:
                d = self.detectors[id]
                if d.getMappedState() != "Ready":
                    ready = False
                    break
            if ready:
                self.transition("configured")

        if self.stateMachine.currentState == "Ready":
            for id in self.activeDetectors:
                d = self.detectors[id]
                #The PCA is not allowed to move into the Running State on his own
                #only ready -> not ready is possible
                if d.getMappedState() == "NotReady" or d.getMappedState() == "Connection Problem":
                    # some Detectors are not ready anymore
                    self.error()

        if self.stateMachine.currentState == "Running":
            for id in self.activeDetectors:
                d = self.detectors[id]
            #Some Detector stopped Working
                if d.getMappedState() != "Running":
                    self.error()

        if self.stateMachine.currentState == "RunningInError":
            countDetectors = 0
            for id in self.activeDetectors:
                d = self.detectors[id]
                if d.getMappedState() == "Running":
                    countDetectors = countDetectors +1
            if countDetectors == self.activeDetectors.size():
                #All Detecotors are working again
                self.transition("resolved")

    def transition(self,command):
        """try to transition the own Statemachine"""
        oldstate = self.stateMachine.currentState
        if self.stateMachine.transition(command):
            self.publishQueue.put((self.id,self.stateMachine.currentState))
            self.log("GLobal Statechange: "+oldstate+" -> "+self.stateMachine.currentState)
        self.checkGlobalState()

    def error(self):
        """make an error transition"""
        self.transition("error")

    def putPendingTransition(self,id,number):
        self.pendingTransitions[id] = number

    def removePendingTransition(self,id):
        if id in self.pendingTransitions:
            del self.pendingTransitions[id]


    def threadFunctionCall(self,id,function,retq):
        """calls a function from Detector(id) und puts result in a given Queue"""
        r = function()
        if retq:
            retq.put((id,r))

    def shutdown(self):
        """tells all Detectors to shutdown"""
        threadArray = []
        returnQueue = Queue()
        if not (len(self.detectors)==self.activeDetectors.size()):
            self.log("Warning: Some Detectors are disconnected")
        self.sem.acquire()
        try:
            for id in self.activeDetectors:
                d = self.detectors[id]
                t = threading.Thread(name='doff'+str(d.id), target=self.threadFunctionCall, args=(d.id, d.powerOff, None))
                threadArray.append(t)
                t.start()

            self.transition("stop")
        except Exception as e:
            self.log("Exception while shutdown: %s" %(str(e),))
        finally:
            self.sem.release()
        return ECSCodes.ok

    def makeReady(self):
        """tells all Detectors to get ready to start"""
        threadArray = []
        returnQueue = Queue()
        self.sem.acquire()
        try:
            for id in self.activeDetectors:
                d = self.detectors[id]
                t = threading.Thread(name='dready'+str(d.id), target=self.threadFunctionCall, args=(d.id, d.getReady, None))
                threadArray.append(t)
                t.start()
        except Exception as e:
            self.log("Exception while readying: %s" %(str(e),))
        finally:
            self.sem.release()
        return ECSCodes.ok

    def start(self):
        """tells all Detectors to start running"""
        if not (len(self.detectors)==self.activeDetectors.size()):
            self.log("Warning: Some Detectors are disconnected")
        self.sem.acquire()
        try:
            if self.stateMachine.currentState != "Ready" and self.stateMachine.currentState != "RunningInError":
                self.log("start not possible in current state")
                return ECSCodes.error
            threadArray = []
            returnQueue = Queue()
            for id in self.activeDetectors:
                d = self.detectors[id]
                t = threading.Thread(name='dstart'+str(d.id), target=self.threadFunctionCall, args=(d.id, d.start, None))
                threadArray.append(t)
                t.start()
            self.transition("start")
        except Exception as e:
            self.log("Exception while starting: %s" %(str(e),))
        finally:
            self.sem.release()
        return ECSCodes.ok

    def stop(self):
        """tells all Detectors to stop running"""
        threadArray = []
        returnQueue = Queue()
        if not (len(self.detectors)==self.activeDetectors.size()):
            self.log("Warning: Some Detectors are disconnected")
        self.sem.acquire()
        try:
            for id in self.activeDetectors:
                d = self.detectors[id]
                t = threading.Thread(name='dstop'+str(d.id), target=self.threadFunctionCall, args=(d.id, d.stop, None))
                threadArray.append(t)
                t.start()

            self.transition("stop")
        except Exception as e:
            self.log("Exception while stopping: %s" %(str(e),))
        finally:
            self.sem.release()
        return ECSCodes.ok

    def abort(self):
        threadArray = []
        returnQueue = Queue()
        self.sem.acquire()
        try:
            for id in self.detectors.keyIterator():
                d = self.detectors[id]
                t = threading.Thread(name='dabort'+str(d.id), target=self.threadFunctionCall, args=(d.id, d.abort, None))
                threadArray.append(t)
                t.start()
        except Exception as e:
            self.log("Exception while aborting: %s" %(str(e),))
        finally:
            self.sem.release()
        return ECSCodes.ok

    def log(self,logmessage,error=False):
        str=datetime.now().strftime("%Y-%m-%d %H:%M:%S")+":" + logmessage
        try:
            self.socketLogPublish.send(str.encode())
        except zmq.error.ZMQError:
            self.socketLogPublish.close()
        if error:
            logging.critical(logmessage)
        else:
            logging.info(logmessage)


    def terminatePCA(self,_signo, _stack_frame):
        self.log("terminating")
        for id in self.detectors.keyIterator():
            d = self.detectors[id]
            d.terminate()
        self.terminate.set()
        #force Queue.get to stop blocking
        self.publishQueue.put((False,False))
        self.commandQueue.put(False)
        self.socketLogPublish.close()
        self.socketPublish.close()
        self.context.term()
        exit(0)

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
        print ("5: abort Transition")
        try:
            x = input()
        except KeyboardInterrupt:
            test.terminatePCA(0,0)
            break
        except EOFError:
            test.terminate.wait()
            test.terminatePCA(0,0)
            break
        if x == "1":
            test.commandQueue.put((ECSCodes.getReady,None))
        if x == "2":
            test.commandQueue.put((ECSCodes.start,None))
        if x== "3":
            test.commandQueue.put((ECSCodes.stop,None))
        if x== "4":
            test.commandQueue.put((ECSCodes.shutdown,None))
        if x== "5":
            test.commandQueue.put((ECSCodes.abort,None))
