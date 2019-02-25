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
from ECSCodes import ECSCodes
codes = ECSCodes()
from states import PCAStates, PCATransitions, MappedStates, CommonStates, DCSStates, FLESStates, QAStates, TFCStates
pcaStates = PCAStates()
pcaTransitions = PCATransitions()
import configparser
import copy
import json
from DataObjects import DataObjectCollection, detectorDataObject, partitionDataObject, stateObject, globalSystemDataObject, configObject
import sys
import PartitionComponents
from PartitionComponents import DCS,TFC,QA,FLES
from ECS_tools import MapWrapper
import ECS_tools
import time
import zc.lockfile #pip3 install zc.lockfile
import signal

class PCA:
    def __init__(self,id):
        self.id = id

        #for time measurement of configuring all Systems
        self.start_time = 0.0
        self.end_time = 0.0

        #create lock
        try:
            self.lock = zc.lockfile.LockFile('/tmp/lock'+self.id, content_template='{pid}')
        except zc.lockfile.LockError:
            print("other Process is already Running "+self.id)
            exit(1)
        self.terminate = threading.Event()
        self.initdone = threading.Event()
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

        def checkConsistencyRequest(detectorList):
            detectorList = DataObjectCollection(json.loads(detectorList),detectorDataObject)
            start_new_thread(self.checkSystemConsistency,(detectorList,))
            return codes.ok

        def measureConfigureTime(arg=None):
            self.start_time = time.time()
            return self.configure(arg)

        #lookup table for recieved commands
        self.functionForCodeDictionary = {
            #codes.getReady: self.configure,
            codes.getReady: measureConfigureTime,
            codes.start: self.startRecording,
            codes.stop: self.stopRecording,
            codes.removeDetector: self.removeDetector,
            codes.addDetector: self.addDetector,
            codes.abort: self.abort,
            codes.check: checkConsistencyRequest,
            codes.lock: self.lockPartition,
            codes.unlock: self.unlockPartition,
            codes.reset: self.resetSystem,
            codes.subsystemMessage: self.handleSystemMessage
        }

        #get your config
        configECS = None
        while configECS == None:
            requestSocket = self.context.socket(zmq.REQ)
            requestSocket.connect("tcp://%s:%s" % (conf['ECSAddress'],conf['ECSRequestPort']))
            requestSocket.setsockopt(zmq.RCVTIMEO, int(conf['receive_timeout']))
            requestSocket.setsockopt(zmq.LINGER,0)

            requestSocket.send_multipart([codes.pcaAsksForConfig, id.encode()])
            try:
                #returns None in pca is unknown
                configJSON = requestSocket.recv()
                if configJSON == codes.idUnknown:
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
        #update number
        self.sequence = 0
        self.transitionNumber = 0
        #the only one who may change the status Map, is the publisher thread
        self.statusMap = MapWrapper()
        self.sem = threading.Semaphore()
        self.publishQueue = Queue()
        self.autoConfigure = False
        self.globalTag = False
        self.partitionLocked = False

        #ZMQ Socket to publish new state Updates
        self.socketPublish = self.context.socket(zmq.PUB)
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

        self.readyDetectorCount = {}
        #get your Detectorlist
        detList = None
        while detList == None:
            requestSocket = self.context.socket(zmq.REQ)
            requestSocket.connect("tcp://%s:%s" % (conf['ECSAddress'],conf['ECSRequestPort']))
            requestSocket.setsockopt(zmq.RCVTIMEO, int(conf['receive_timeout']))
            requestSocket.setsockopt(zmq.LINGER,0)

            requestSocket.send_multipart([codes.pcaAsksForDetectorList, self.id.encode()])
            try:
                #receive detectors as json
                ret = requestSocket.recv()
                if ret == codes.error:
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

        self.stateMachine = Statemachine(conf["stateMachineCSV"],PCAStates.Idle)

        start_new_thread(self.publisher,())

        #Subscribers need some time to subscribe
        time.sleep(1)
        #tell subscribers to reset their state Table
        self.publishQueue.put((self.id,codes.reset))

        #create global System objects
        systemList = ["TFC","DCS","QA","FLES"]
        res = []
        self.messageHandlerFunctionForSystem = {
            "DCS" : self.handleDCSMessage,
        }
        #get info from ECS
        for s in systemList:
            requestSocket = self.context.socket(zmq.REQ)
            requestSocket.connect("tcp://%s:%s" % (conf['ECSAddress'],conf['ECSRequestPort']))
            requestSocket.setsockopt(zmq.RCVTIMEO, int(conf['receive_timeout']))
            requestSocket.setsockopt(zmq.LINGER,0)

            requestSocket.send_multipart([codes.GlobalSystemAsksForInfo, s.encode()])
            try:
                #receive detectors as json
                ret = requestSocket.recv()
                if ret == codes.error:
                    self.log("received error getting GlobalSystem %s" % s, True)
                    exit(1)
                if ret == codes.idUnknown:
                    self.log("ECS does not know GlobalSystem %s" % s, True)
                    exit(1)
                JSON = json.loads(ret.decode())
                #create DataObjectCollection from JSON
                res.append(globalSystemDataObject(JSON))
            except zmq.Again:
                self.log("timeout getting GlobalSystem %s" % s, True)
                exit(1)
            except zmq.error.ContextTerminated:
                pass
            finally:
                requestSocket.close()
        #create Objects
        tfcData, dcsData, qaData, flesData = res
        self.globalSystems = {}
        self.TFC = TFC(self.id,tfcData.address,tfcData.portCommand,"TFC",self.log,self.globalSystemTimeout,self.globalSystemReconnect)
        self.globalSystems["TFC"] = self.TFC
        self.DCS = DCS(self.id,dcsData.address,dcsData.portCommand,"DCS",self.log,self.globalSystemTimeout,self.globalSystemReconnect)
        self.globalSystems["DCS"] = self.DCS
        self.QA = QA(self.id,qaData.address,qaData.portCommand,"QA",self.log,self.globalSystemTimeout,self.globalSystemReconnect)
        self.globalSystems["QA"] = self.QA
        self.FLES = FLES(self.id,flesData.address,flesData.portCommand,"FLES",self.log,self.globalSystemTimeout,self.globalSystemReconnect)
        self.globalSystems["FLES"] = self.FLES

        #maps configure Functions to their corresponding PCA State
        self.configureFunctionForState = {
            PCAStates.Idle : self.TFC.getReady,
            PCAStates.TFC_Active : self.makeDetectorsReady,
            PCAStates.Detectors_Active : self.configureDCSandFLES,
            PCAStates.FLES_and_DCS_Active: self.QA.getReady,
        }

        #create Detector objects
        threadArray = []
        for d in detList:
            #in case there of a connection problem, creating a Detector might take a long time, therefore create a own thread for each detector
            t = threading.Thread(name='dcreate'+str(d.id), target=self.addDetector, args=(d,))
            threadArray.append(t)
            t.start()
        for t in threadArray:
            t.join()
        self.publishQueue.put((self.id,stateObject([self.stateMachine.currentState,self.stateMachine.currentState,self.globalTag,None])))
        #thread stuff
        start_new_thread(self.waitForUpdates,())
        start_new_thread(self.waitForStateTableRequests,())
        start_new_thread(self.waitForCommands,())
        self.initdone.set()

    def getCurrentglobalTag(self):
        return self.globalTag

    def waitForCommands(self):
        """wait for an external(start,stop etc.) command e.g.from the WebGUI"""
        while True:
            try:
                command = self.remoteCommandSocket.recv_multipart()
                arg = None
                if len(command) > 1:
                    arg = command[1].decode()
                command = command[0]

                if command==codes.ping:
                    self.remoteCommandSocket.send(codes.ok)
                    #it's just a ping
                    continue

                #returns function for Code or None if the received code is unknown
                f = self.functionForCodeDictionary.get(command,None)
                if not f:
                    self.remoteCommandSocket.send(codes.unknownCommand)
                    continue
                if arg:
                    self.remoteCommandSocket.send(f(arg))
                else:
                    self.remoteCommandSocket.send(f())
            except zmq.error.ContextTerminated:
                self.remoteCommandSocket.close()
                break



    def publisher(self):
        """publishes all state changes inside the Queue, updates the PCA Statemap and checks current PCA State"""
        self.initdone.wait()
        while True:
            id,state = self.publishQueue.get()
            if self.terminate.is_set():
                break
            self.sequence = self.sequence + 1
            if state == codes.removed:
                del self.statusMap[id]
            elif state != codes.reset:
                self.statusMap[id] = (self.sequence,state)
                self.checkGlobalState(id)
            #print(id,self.sequence,state)
            ECS_tools.send_status(self.socketPublish,id,self.sequence,state)


    def waitForUpdates(self):
        """wait for updates from Detectors"""
        while True:
            try:
                message = self.socketDetectorUpdates.recv()
                message = json.loads(message.decode())
                id = message["id"]

                if id in self.detectors:
                    subSystemObject = self.detectors[id]
                elif id in self.globalSystems:
                    subSystemObject = self.globalSystems[id]
                else:
                    self.log("received message with unknown id: %s" % id,True)
                    self.socketDetectorUpdates.send(codes.idUnknown)
                    continue
                self.socketDetectorUpdates.send(codes.ok)

                if not subSystemObject.checkSequence(message["sequenceNumber"]):
                    #update is obsolete
                    continue

                state = message["state"]
                configTag = None
                comment = None
                if "tag" in message:
                    configTag = message["tag"]
                if "comment" in message:
                    comment = message["comment"]
                if (subSystemObject.stateMachine.currentState == state):
                    continue
                oldstate = subSystemObject.stateMachine.currentState
                subSystemObject.setState(state,configTag,comment)
                self.publishQueue.put((id,subSystemObject.getStateObject()))
                #if Detector was never connected oldstate will be False (no log necessary)
                if oldstate:
                    self.log("%s Transition done %s -> %s" % (subSystemObject.name,oldstate,subSystemObject.stateMachine.currentState))
            except zmq.error.ContextTerminated:
                self.socketDetectorUpdates.close()
                break

    def waitForStateTableRequests(self):
        """waits for requests for entire State-Table"""
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
                ret = d.getStateFromSystem()
                if not ret:
                    #timeout getting State let the pingThread handle it
                    continue
                state,configTag = ret
                if d.stateMachine.currentState != state:
                    self.log("During System check Detector %s returned an unexpected state: %s" % (d.id,state),True)
                    d.setState(state,configTag,"found by consistency check")
                if d.currentStateObject.configTag != configTag:
                    self.log("During System check Detector %s returned an unexpected configuration Tag: %s" % (d.id,configTag),True)
                    d.currentStateObject.configTag = configTag

    def addDetector(self,detector):
        """add Detector to Dictionary and pubish it's state"""
        #detector list can only be changed in Idle State
        if pcaStates.isActiveState(self.stateMachine.currentState):
            return codes.busy
        self.sem.acquire()
        try:
            if isinstance(detector,str):
                detector = detectorDataObject(json.loads(detector))
            #create the corresponding class for the specified type
            types = PartitionComponents.DetectorTypes()
            typeClass = types.getClassForType(detector.type)
            if not typeClass:
                return False
            #get the config Section for the Detector type
            confSection = types.getConfsectionForType(detector.type)
            det = typeClass(detector.id,detector.address,detector.portCommand,confSection,self.log,self.handleDetectorTimeout,self.handleDetectorReconnect)
            self.detectors[det.id] = det
            #global state doesn't exist during start up
            if self.stateMachine:
                start_new_thread(self.transitionDetectorIntoGlobalState,(det.id,))
        except Exception as e:
            self.log("Exception while adding Detector %s: %s" %(detector.id,str(e)))
        finally:
            self.sem.release()
        return codes.ok

    def removeDetector(self,id):
        """remove Detector from Dictionary"""
        if pcaStates.isActiveState(self.stateMachine.currentState):
            return codes.busy
        self.sem.acquire()
        try:
            if id not in self.detectors:
                self.log("Detector with id %s is unknown" % id,True)
                return codes.idUnknown
            det = self.detectors[id]
            self.publishQueue.put((id,codes.removed))
            del self.detectors[id]
            del self.readyDetectorCount[id]
            #this might take a few seconds dending on ping interval
            start_new_thread(det.terminate,())
        except Exception as e:
            self.log("Exception while removing Detector %s: %s" %(id,str(e)))
        finally:
            self.sem.release()
        return codes.ok

    def globalSystemTimeout(self,id):
        """this function is triggered if a global System timesout"""
        gs = self.globalSystems[id]
        self.publishQueue.put((id,gs.getStateObject()))

    def globalSystemReconnect(self,id):
        """this function is triggered if a global System connects"""
        gs = self.globalSystems[id]
        stateObj = gs.getStateObject()
        self.publishQueue.put((id,stateObj))
        if stateObj.configTag and self.getCurrentglobalTag() != stateObj.configTag:
            gs.abort()

    def handleSystemMessage(self,arg):
        message = json.loads(arg)

        if "id" not in message:
            self.log("received malformed system message: %s" % str(message),True)
            return codes.error
        id = message["id"]
        if id in self.messageHandlerFunctionForSystem:
            self.messageHandlerFunctionForSystem[id](message)
        else:
            self.log("received system message from %s but not message handler found" % id,True)
            return codes.idUnknown
        return codes.ok

    def handleDCSMessage(self,message):
        """handle messages from the DCS"""
        detId = message["detectorId"]
        message = message["message"]
        det = self.detectors[detId]
        self.log("received message %s from DCS for Detector %s" % (message,detId))
        if message == "detectorError":
            det.error()

    def handleDetectorTimeout(self,id):
        """this function is triggered if a Detector timesout"""
        detector = self.detectors[id]
        self.publishQueue.put((id,detector.getStateObject()))

    def handleDetectorReconnect(self,id,stateObj):
        """this function is triggered if a Detector Configuring_Detectors"""
        if id in self.detectors:
            self.publishQueue.put((id,stateObj))
            #abort Detector if it has a different config Tag than the PCA
            if stateObj.configTag and self.getCurrentglobalTag() != stateObj.configTag:
                det = self.detectors[id]
                det.abort()
            self.transitionDetectorIntoGlobalState(id)

    def transitionDetectorIntoGlobalState(self,id):
        """try to transition Detector to global State of the PCA"""
        det = self.detectors[id]
        if self.stateMachine.currentState == PCAStates.Recording:
            det.getReady()

    def error_transition(self,transition):
        """error transition for PCA"""
        if self.stateMachine.checkIfPossible(transition):
            if self.stateMachine.currentState == PCAStates.Recording:
                self.stopRecording()
            self.transition(transition)

    def checkGlobalState(self,id):
        """after a received State Update from id checks wether current state is still valid
         and performs a PCA Statemachien transition if necessary"""
        self.globalSystemsForConfigureState = {
            PCAStates.Configuring_TFC : ("TFC",),
            PCAStates.Configuring_QA : ("QA",),
            PCAStates.Configuring_FLES_and_DCS : ("FLES","DCS"),
        }
        #global Stata change just happened
        if id == self.id:
            #check if system is already configured
            if self.stateMachine.currentState in self.globalSystemsForConfigureState:
                ready = True
                for gs in self.globalSystemsForConfigureState[self.stateMachine.currentState]:
                    gs = self.globalSystems[gs]
                    if gs.getMappedState() != MappedStates.Active:
                        ready = False
                if ready:
                    self.transition(PCATransitions.success)
                    if self.autoConfigure:
                        if self.stateMachine.currentState in self.configureFunctionForState and self.configureFunctionForState[self.stateMachine.currentState]():
                            self.transition(PCATransitions.configure)
            #check if all Detectors are configured
            elif self.stateMachine.currentState == PCAStates.Configuring_Detectors:
                for id in self.detectors.keyIterator():
                    d = self.detectors[id]
                    #if d.getMappedState() == MappedStates.Active:
                    self.readyDetectorCount[d.id] = (d.getMappedState() == MappedStates.Active)
                if all(v == True for k,v in self.readyDetectorCount.items()):
                    self.transition(PCATransitions.success)
                    if self.autoConfigure:
                        if self.configureFunctionForState[self.stateMachine.currentState]():
                            self.transition(PCATransitions.configure)
            #after stop transition check if all Detectors are still ready(detector error won't stop the Recording)
            elif self.stateMachine.currentState == PCAStates.QA_Active:
                for id in self.detectors.keyIterator():
                    d = self.detectors[id]
                    if d.getMappedState() != MappedStates.Active:
                        self.error_transition(PCATransitions.error_Detector)
        #TFC State Change happened
        elif id == "TFC":
            #if PCA in configuring TFC
            if self.stateMachine.currentState == PCAStates.Configuring_TFC:
                #configure successfull
                if self.TFC.getMappedState() == MappedStates.Active:
                    self.transition(PCATransitions.success)
                    if self.autoConfigure:
                        if self.configureFunctionForState[self.stateMachine.currentState]():
                            self.transition(PCATransitions.configure)
                        #self.configure()
                #configuring failed
                elif self.TFC.getMappedState() == MappedStates.Unconfigured:
                    self.transition(PCATransitions.failure)
            #TFC is not active
            elif self.TFC.getMappedState() in {MappedStates.Unconfigured,CommonStates.ConnectionProblem,MappedStates.Error}:
                self.error_transition(PCATransitions.error_TFC)
            #TFC started configuring
            elif self.TFC.getMappedState() == MappedStates.Configuring:
                self.transition(PCATransitions.configure)
        #Detector State changed
        elif id in self.detectors:
            det = self.detectors[id]
            self.readyDetectorCount[id] = (det.getMappedState() == MappedStates.Active)
            #if configuring
            if self.stateMachine.currentState == PCAStates.Configuring_Detectors and self.readyDetectorCount[id]:
                if all(v == True for k,v in self.readyDetectorCount.items()):
                    self.transition(PCATransitions.success)
                    self.log("detector configure time"+str(time.time()-self.detector_configure_time_start))
                    if self.autoConfigure:
                        if self.configureFunctionForState[self.stateMachine.currentState]():
                            self.transition(PCATransitions.configure)
            #detector is not active
            elif det.getMappedState() in {MappedStates.Unconfigured,CommonStates.ConnectionProblem,MappedStates.Error}:
                if self.stateMachine.currentState == PCAStates.Configuring_Detectors:
                    self.transition(PCATransitions.failure)
                else:
                    self.error_transition(PCATransitions.error_Detector)
            #a detector started configuring
            elif det.getMappedState() == MappedStates.Configuring:
                self.transition(PCATransitions.configure)
        #FLES or DCS changed it's state
        elif id in {"FLES","DCS"}:
            if self.stateMachine.currentState == PCAStates.Configuring_FLES_and_DCS:
                #are both systems ready?
                if self.FLES.getMappedState() == MappedStates.Active and self.DCS.getMappedState() == MappedStates.Active:
                    self.transition(PCATransitions.success)
                    if self.autoConfigure:
                        if self.configureFunctionForState[self.stateMachine.currentState]():
                            self.transition(PCATransitions.configure)
                #one of them failed
                elif (id == "FLES" and self.FLES.getMappedState() == MappedStates.Unconfigured) or (id == "DCS" and self.DCS.getMappedState() == MappedStates.Unconfigured):
                        self.transition(PCATransitions.failure)
            #FLES OR DCS is not ready anymore
            elif self.FLES.getMappedState() in {MappedStates.Unconfigured,CommonStates.ConnectionProblem,MappedStates.Error} or self.DCS.getMappedState() in {MappedStates.Unconfigured,CommonStates.ConnectionProblem,MappedStates.Error}:
                self.error_transition(PCATransitions.error_FLES_OR_DCS)
                if id == "FLES":
                    #FLES should only be recording if PCA is in recording State
                    if self.FLES.stateMachine.currentState == FLESStates.Recording and self.stateMachine.currentState != PCAStates.Recording:
                        self.FLES.stopRecording()
            elif self.FLES.getMappedState() == MappedStates.Configuring or self.DCS.getMappedState() == MappedStates.Configuring:
                self.transition(PCATransitions.configure)
        #QA Status changed
        elif id == "QA":
            if self.stateMachine.currentState == PCAStates.Configuring_QA:
                #configure success
                if self.QA.getMappedState() == MappedStates.Active:
                    self.transition(PCATransitions.success)
                    self.end_time = time.time()
                    self.log("configure Time:"+str(self.end_time-self.start_time))
                #configure failure
                elif self.QA.getMappedState() == MappedStates.Unconfigured:
                    self.transition(PCATransitions.failure)
            #QA is not active anymore
            elif self.QA.getMappedState() in {MappedStates.Unconfigured,CommonStates.ConnectionProblem,MappedStates.Error}:
                self.error_transition(PCATransitions.error_QA)
                #QA shoudn't record if PCA is not in recording State
            elif self.QA.stateMachine.currentState == QAStates.Recording and self.stateMachine.currentState != PCAStates.Recording:
                self.QA.stopRecording()
            elif self.QA.getMappedState() == MappedStates.Configuring:
                self.transition(PCATransitions.configure)

    def transition(self,command):
        """try to transition the own Statemachine"""
        oldstate = self.stateMachine.currentState
        if self.stateMachine.transition(command):
            #self.publishQueue.put((self.id,stateObject(self.stateMachine.currentState)))
            self.publishQueue.put((self.id,stateObject([self.stateMachine.currentState,self.stateMachine.currentState,self.globalTag,None])))
            self.log("GLobal Statechange: "+oldstate+" -> "+self.stateMachine.currentState)

    def lockPartition(self):
        """lock Partition (locked partitions refuse all incoming commands)"""
        self.partitionLocked = True
        self.log("Partition locked by ECS")
        return codes.ok

    def unlockPartition(self):
        """unlock Parition"""
        self.partitionLocked = False
        self.log("Partition unlocked by ECS")
        return codes.ok

    def configure(self,arg=None):
        """perform a configure step"""
        if self.partitionLocked:
            self.log("Can't configure because Partition is locked")
            return codes.error
        if arg:
            arg = json.loads(arg)
            if arg["autoConfigure"] == "true":
                self.autoConfigure = True
            else:
                self.autoConfigure = False
            systemConfig = DataObjectCollection(json.loads(arg["systemConfig"]),configObject)
            for conf in systemConfig:
                if conf.systemId in self.detectors:
                    sys=self.detectors[conf.systemId]
                    oldconfig = sys.getSystemConfig()
                    if oldconfig and oldconfig.configId != conf.configId and sys.getState():
                        self.transition(PCATransitions.error_Detector)

                elif conf.systemId in self.globalSystems:
                    sys=self.globalSystems[conf.systemId]
                    oldconfig = sys.getSystemConfig()
                    if oldconfig and oldconfig.configId != conf.configId and sys.getState():
                        self.transition(pcaTransitions.errorTransitionForSystem(conf.systemId))
                else:
                    self.log("System %s is in received Config but is not in this partition",True)
                if sys:
                    sys.setSystemConfig(conf)
            self.globalTag = arg["globalTag"]
        if self.stateMachine.currentState == PCAStates.Recording:
            for id in self.detectors.keyIterator():
                d = self.detectors[id]
                if d.getMappedState() != MappedStates.Active:
                    d.getReady(globalTag)
            return ECSCodes.ok
        elif self.stateMachine.currentState == PCAStates.QA_Active:
            return ECSCodes.ok
        elif not self.stateMachine.checkIfPossible(PCATransitions.configure):
            return codes.error
        function = self.configureFunctionForState[self.stateMachine.currentState]
        if function():
            return codes.ok
        return codes.error

    def startRecording(self):
        """start taking Data"""
        if self.partitionLocked:
            self.log("Can't start because Partition is locked")
            return codes.error
        if not self.stateMachine.checkIfPossible(PCATransitions.start_recording):
            return codes.error
        self.transition(PCATransitions.start_recording)
        self.QA.startRecording()
        self.FLES.startRecording()
        return codes.ok

    def stopRecording(self):
        """stop taking Data"""
        if self.partitionLocked:
            self.log("Can't stop because Partition is locked")
            return codes.error
        if not self.stateMachine.checkIfPossible(PCATransitions.stop_recording):
            return codes.error
        self.transition(PCATransitions.stop_recording)
        self.QA.stopRecording()
        self.FLES.stopRecording()
        return codes.ok

    def configureDCSandFLES(self):
        if not self.DCS.getReady():
            return False
        if not self.FLES.getReady():
            return False
        return True

    def resetSystem(self,arg):
        arg = json.loads(arg)
        systemId = arg["systemId"]
        if systemId in self.detectors:
            sys = self.detectors[systemId]
        elif systemId in self.globalSystems:
            sys = self.globalSystems[systemId]
        else:
            return codes.idUnknown

        if sys.reset():
            return codes.ok
        return codes.error

    def threadFunctionCall(self,id,function,retq):
        """calls a function from Detector(id) und puts result in a given Queue"""
        r = function()
        if retq:
            retq.put(r)

    def makeDetectorsReady(self):
        """tells all Detectors to get ready to start"""
        self.detector_configure_time_start = time.time()
        threadArray = []
        #Queue might be killed by garbage collection if one Thread returend False; if it is not set as class attribute
        self.returnQueue = Queue()
        self.sem.acquire()
        try:
            for id in self.detectors.keyIterator():
                d = self.detectors[id]
                t = threading.Thread(name='dready'+str(d.id), target=self.threadFunctionCall, args=(d.id, d.getReady,self.returnQueue))
                threadArray.append(t)
                t.start()
        except Exception as e:
            self.log("Exception while readying: %s" %(str(e),))
            raise e
        finally:
            self.sem.release()
        for t in threadArray:
            if not self.returnQueue.get():
                return False
        return True

    def abort(self):
        """abort all Systems and Detectors"""
        threadArray = []
        returnQueue = Queue()
        self.sem.acquire()
        try:
            self.globalTag=False
            self.transition(PCATransitions.abort)
            for id in self.detectors.keyIterator():
                d = self.detectors[id]
                t = threading.Thread(name='dabort'+str(d.id), target=self.threadFunctionCall, args=(d.id, d.abort, None))
                threadArray.append(t)
                t.start()
            self.TFC.abort()
            self.DCS.abort()
            self.QA.abort()
            self.FLES.abort()
        except Exception as e:
            self.log("Exception while aborting: %s" %(str(e),))
        finally:
            self.sem.release()
        return codes.ok

    def log(self,logmessage,error=False):
        str=datetime.now().strftime("%Y-%m-%d %H:%M:%S")+":" + logmessage
        try:
            #publish log on zmq socket
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
        try:
            x = input()
        except KeyboardInterrupt:
            test.terminatePCA(0,0)
            break
        except EOFError:
            test.terminate.wait()
            test.terminatePCA(0,0)
            break
