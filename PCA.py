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
from states import PCAStates, PCATransitions, MappedStates, DCSStates,DCSTransitions, DetectorStates, DetectorTransitions, FLESStates, FLESTransitions, QAStates, QATransitions, TFCStates, TFCTransitions, GlobalSystemStates, GlobalSystemStatesTransitions
pcaStates = PCAStates()
import configparser
import copy
import json
from DataObjects import DataObjectCollection, detectorDataObject, partitionDataObject, stateObject, globalSystemDataObject
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
        self.commandQueue = Queue()
        self.autoConfigure = False
        self.configTag = False
        self.partitionLocked = False

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

        #Subscribers need some time to subscribe (todo there has to be a better way)
        time.sleep(1)
        #tell subscribers to reset their state Table
        self.publishQueue.put((self.id,codes.reset))

        #create global System objects
        systemList = ["TFC","DCS","QA","FLES"]
        res = []
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

        self.configureFunctionForState = {
            PCAStates.Idle : self.TFC.getReady,
            PCAStates.TFC_Active : self.makeDetectorsReady,
            PCAStates.Detectors_Active : self.makeDCSandFLESReady,
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
        self.publishQueue.put((self.id,stateObject([self.stateMachine.currentState,self.stateMachine.currentState,self.configTag,None])))
        #thread stuff
        start_new_thread(self.waitForUpdates,())
        start_new_thread(self.waitForRequests,())
        start_new_thread(self.waitForRemoteCommand,())
        start_new_thread(self.watchCommandQueue,())
        self.initdone.set()

    def watchCommandQueue(self):
        """watch the command Queue und execute incoming commands"""
        while True:
            m = self.commandQueue.get()
            if self.terminate.is_set():
                break
            command,arg = m
            if command==codes.getReady:
                self.configure()
                continue
            if command == codes.removeDetector:
                self.removeDetector(arg)
                continue
            if command == codes.addDetector:
                detector = detectorDataObject(json.loads(arg))
                self.addDetector(detector)
                continue
            if command == codes.abort:
                self.abort()
                continue

    def getCurrentConfigTag(self):
        return self.configTag

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

                if command==codes.ping:
                    self.remoteCommandSocket.send(codes.ok)
                    #it's just a ping
                    continue

                def checkConsistencyRequest(detectorList):
                    detectorList = DataObjectCollection(json.loads(detectorList),detectorDataObject)
                    start_new_thread(self.checkSystemConsistency,(detectorList,))
                    return codes.ok

                def switcher(code,arg=None):
                    #functions for codes
                    dbFunctionDictionary = {
                        codes.getReady: self.configure,
                        codes.start: self.startRecording,
                        codes.stop: self.stopRecording,
                        codes.removeDetector: self.removeDetector,
                        codes.addDetector: self.addDetector,
                        codes.abort: self.abort,
                        codes.check: checkConsistencyRequest,
                        codes.lock: self.lockPartition,
                        codes.unlock: self.unlockPartition,
                    }
                    #returns function for Code or None if the received code is unknown
                    f = dbFunctionDictionary.get(code,None)
                    if not f:
                        self.remoteCommandSocket.send(codes.unknownCommand)
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
                if id not in self.detectors and id not in {"TFC","DCS","QA","FLES"}:
                    self.log("received message with unknown id: %s" % id,True)
                    self.socketDetectorUpdates.send(codes.idUnknown)
                    continue
                self.socketDetectorUpdates.send(codes.ok)

                if id == "DCS" and "detectorId" in message:
                    self.handleDCSMessage(message["detectorId"],message["message"])
                    continue

                state = message["state"]
                configTag = None
                comment = None
                if "tag" in message:
                    configTag = message["tag"]
                if "comment" in message:
                    comment = message["comment"]
                if id in self.detectors:
                    det = self.detectors[id]
                else:
                    det = self.globalSystems[id]
                if (det.stateMachine.currentState == state):
                    continue
                oldstate = det.stateMachine.currentState
                det.setState(state,configTag,comment)
                self.publishQueue.put((id,det.getStateObject()))
                #if Detector was never connected oldstate will be False (no log necessary)
                if oldstate:
                    self.log("%s Transition done %s -> %s" % (det.name,oldstate,det.stateMachine.currentState))
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
                ret = d.getStateFromDetector()
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
            print(self.stateMachine.currentState)
            return codes.busy
        self.sem.acquire()
        #try:
        if isinstance(detector,str):
            detector = detectorDataObject(json.loads(detector))
        #create the corresponding class for the specified type
        types = PartitionComponents.DetectorTypes()
        typeClass = types.getClassForType(detector.type)
        if not typeClass:
            return False
        confSection = types.getConfsectionForType(detector.type)
        det = typeClass(detector.id,detector.address,detector.portTransition,detector.portCommand,confSection,self.log,self.handleDetectorTimeout,self.handleDetectorReconnect)
        self.detectors[det.id] = det
        #self.publishQueue.put((det.id,det.getStateObject()))
        #global state doesn't exist during start up
        if self.stateMachine:
            start_new_thread(self.transitionDetectorIntoGlobalState,(det.id,))
        #except Exception as e:
        #    self.log("Exception while adding Detector %s: %s" %(detector.id,str(e)))
        #finally:
        self.sem.release()
        return codes.ok

    def removeDetector(self,id):
        """remove Detector from Dictionary"""
        if pcaStates.isActiveState(self.stateMachine.currentState):
            print(self.stateMachine.currentState)
            return codes.busy
        self.sem.acquire()
        try:
            if id not in self.detectors:
                self.log("Detector with id %s is unknown" % id,True)
                return codes.idUnknown
            det = self.detectors[id]
            #todo add possibility to cancel transitions?
            self.publishQueue.put((id,codes.removed))
            del self.detectors[id]
            #this might take a few seconds dending on ping interval
            start_new_thread(det.terminate,())
        except Exception as e:
            self.log("Exception while removing Detector %s: %s" %(id,str(e)))
        finally:
            self.sem.release()
        return codes.ok

    def globalSystemTimeout(self,id):
        gs = self.globalSystems[id]
        self.publishQueue.put((id,gs.getStateObject()))

    def globalSystemReconnect(self,id):
        gs = self.globalSystems[id]
        stateObj = gs.getStateObject()
        self.publishQueue.put((id,stateObj))
        if stateObj.configTag and self.getCurrentConfigTag() != stateObj.configTag:
            gs.abort()

    def handleDCSMessage(self,detId,message):
        det = self.detectors[detId]
        self.log("received message %s from DCS for Detector %s" % (message,detId))
        if message == "critical":
            det.abort()

    def handleDetectorTimeout(self,id):
        #wait until Statemachine initialised
        detector = self.detectors[id]
        self.publishQueue.put((id,detector.getStateObject()))

    def handleDetectorReconnect(self,id,stateObj):
        #if detector is active try to transition him into globalState
        if id in self.detectors:
            self.publishQueue.put((id,stateObj))
            print(stateObj.configTag)
            if stateObj.configTag and self.getCurrentConfigTag() != stateObj.configTag:
                det = self.detectors[id]
                det.abort()
            self.transitionDetectorIntoGlobalState(id)

    def transitionDetectorIntoGlobalState(self,id):
        """try to transition Detector to global State of the PCA"""
        det = self.detectors[id]
        if self.stateMachine.currentState == PCAStates.Recording:
            det.getReady()

    def error_transition(self,transition):
        if self.stateMachine.checkIfPossible(transition):
            if self.stateMachine.currentState == PCAStates.Recording:
                self.stopRecording()
            self.transition(transition)

    def checkGlobalState(self,id):
        #globalStateChange
        self.globalSystemsForConfigureState = {
            PCAStates.Configuring_TFC : ("TFC",),
            PCAStates.Configuring_QA : ("QA",),
            PCAStates.Configuring_FLES_and_DCS : ("FLES","DCS"),
        }
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
                        self.configure()
            elif self.stateMachine.currentState == PCAStates.Configuring_Detectors:
                ready = True
                for id in self.detectors.keyIterator():
                    d = self.detectors[id]
                    if d.getMappedState() != MappedStates.Active:
                        ready = False
                        break
                if ready:
                    self.transition(PCATransitions.success)
                    if self.autoConfigure:
                        self.configure()
            #after stop transition check if all Detectors are still ready(detector error won't stop the Recording)
            elif self.stateMachine.currentState == PCAStates.QA_Active:
                for id in self.detectors.keyIterator():
                    d = self.detectors[id]
                    if d.getMappedState() != MappedStates.Active:
                        self.error_transition(PCATransitions.error_Detector)
        elif id == "TFC":
            if self.stateMachine.currentState == PCAStates.Configuring_TFC:
                if self.TFC.getMappedState() == MappedStates.Active:
                    self.transition(PCATransitions.success)
                    if self.autoConfigure:
                        self.configure()
                elif self.TFC.getMappedState() == MappedStates.Unconfigured:
                    self.transition(PCATransitions.failure)
            elif self.TFC.getMappedState() == MappedStates.Unconfigured or self.TFC.getMappedState() == TFCStates.ConnectionProblem:
                self.error_transition(PCATransitions.error_TFC)
        elif id in self.detectors:
            det = self.detectors[id]
            if self.stateMachine.currentState == PCAStates.Configuring_Detectors and det.getMappedState() == MappedStates.Active :
                ready = True
                for id in self.detectors.keyIterator():
                    d = self.detectors[id]
                    if d.getMappedState() != MappedStates.Active:
                        ready = False
                        break
                if ready:
                    self.transition(PCATransitions.success)
                    if self.autoConfigure:
                        self.configure()
            elif det.getMappedState() == MappedStates.Unconfigured or det.getMappedState() == DetectorStates.ConnectionProblem:
                if self.stateMachine.currentState == PCAStates.Configuring_Detectors:
                    self.transition(PCATransitions.failure)
                else:
                    self.error_transition(PCATransitions.error_Detector)
        elif id in {"FLES","DCS"}:
            if self.stateMachine.currentState == PCAStates.Configuring_FLES_and_DCS:
                if self.FLES.getMappedState() == MappedStates.Active and self.DCS.getMappedState() == MappedStates.Active:
                    self.transition(PCATransitions.success)
                    if self.autoConfigure:
                        self.configure()
                elif (id == "FLES" and self.FLES.getMappedState() == MappedStates.Unconfigured) or (id == "DCS" and self.DCS.getMappedState() == MappedStates.Unconfigured):
                        self.transition(PCATransitions.failure)
            elif self.FLES.getMappedState() == MappedStates.Unconfigured or self.FLES.getMappedState() == FLESStates.ConnectionProblem or self.DCS.getMappedState() == MappedStates.Unconfigured or self.DCS.getMappedState() == DCSStates.ConnectionProblem:
                self.error_transition(PCATransitions.error_FLES_OR_DCS)
            if id == "FLES":
                if self.FLES.stateMachine.currentState == FLESStates.Recording and self.stateMachine.currentState != PCAStates.Recording:
                    self.FLES.stopRecording()
        elif id == "QA":
            if self.stateMachine.currentState == PCAStates.Configuring_QA:
                if self.QA.getMappedState() == MappedStates.Active:
                    self.transition(PCATransitions.success)
                    if self.autoConfigure:
                        self.configure()
                elif self.QA.getMappedState() == MappedStates.Unconfigured:
                    self.transition(PCATransitions.failure)
            elif self.QA.getMappedState() == MappedStates.Unconfigured or self.QA.getMappedState() == QAStates.ConnectionProblem:
                self.error_transition(PCATransitions.error_QA)
            elif self.QA.stateMachine.currentState == QAStates.Recording and self.stateMachine.currentState != PCAStates.Recording:
                self.QA.stopRecording()


    def transition(self,command):
        """try to transition the own Statemachine"""
        oldstate = self.stateMachine.currentState
        if self.stateMachine.transition(command):
            #self.publishQueue.put((self.id,stateObject(self.stateMachine.currentState)))
            self.publishQueue.put((self.id,stateObject([self.stateMachine.currentState,self.stateMachine.currentState,self.configTag,None])))
            self.log("GLobal Statechange: "+oldstate+" -> "+self.stateMachine.currentState)

    def lockPartition(self):
        print("Parition locked")
        self.partitionLocked = True
        return codes.ok

    def unlockPartition(self):
        print("Parition unlocked")
        self.partitionLocked = False
        return codes.ok

    def configure(self,arg=None):
        if self.partitionLocked:
            self.log("Can't configure because Partition is locked")
            return codes.error
        if arg:
            arg = json.loads(arg)
            if arg["autoConfigure"] == "true":
                self.autoConfigure = True
            else:
                self.autoConfigure = False
            if self.configTag != arg["configTag"]:
                self.abort()
            self.configTag = arg["configTag"]
        if self.stateMachine.currentState == PCAStates.Recording:
            for id in self.detectors.keyIterator():
                d = self.detectors[id]
                if d.getMappedState() != MappedStates.Active:
                    d.getReady(configTag)
            return ECSCodes.ok
        elif not self.stateMachine.checkIfPossible(PCATransitions.configure):
            return codes.error
        function = self.configureFunctionForState[self.stateMachine.currentState]
        if function(self.configTag):
            self.transition(PCATransitions.configure)
            return codes.ok
        return codes.error

    def startRecording(self):
        if not self.stateMachine.checkIfPossible(PCATransitions.start_recording):
            return codes.error
        self.transition(PCATransitions.start_recording)
        self.QA.startRecording()
        self.FLES.startRecording()
        return codes.ok

    def stopRecording(self):
        if not self.stateMachine.checkIfPossible(PCATransitions.stop_recording):
            return codes.error
        self.transition(PCATransitions.stop_recording)
        self.QA.stopRecording()
        self.FLES.stopRecording()
        return codes.ok

    def makeDCSandFLESReady(self,configTag):
        if not self.DCS.getReady(configTag):
            return False
        if not self.FLES.getReady(configTag):
            return False
        return True


    def error(self):
        """make an error transition"""
        self.transition("error")

    def threadFunctionCall(self,id,function,configTag,retq):
        """calls a function from Detector(id) und puts result in a given Queue"""
        if configTag:
            r = function(configTag)
        else:
            r = function()
        if retq:
            retq.put(r)

    def makeDetectorsReady(self,configTag):
        """tells all Detectors to get ready to start"""
        threadArray = []
        returnQueue = Queue()
        self.sem.acquire()
        try:
            for id in self.detectors.keyIterator():
                d = self.detectors[id]
                t = threading.Thread(name='dready'+str(d.id), target=self.threadFunctionCall, args=(d.id, d.getReady,configTag,returnQueue))
                threadArray.append(t)
                t.start()
        except Exception as e:
            self.log("Exception while readying: %s" %(str(e),))
            raise e
        finally:
            self.sem.release()
        for t in threadArray:
            if not returnQueue.get():
                return False
        return True

    def abort(self):
        threadArray = []
        returnQueue = Queue()
        self.sem.acquire()
        try:
            self.configTag=False
            self.transition(PCATransitions.abort)
            for id in self.detectors.keyIterator():
                d = self.detectors[id]
                t = threading.Thread(name='dabort'+str(d.id), target=self.threadFunctionCall, args=(d.id, d.abort,None, None))
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
            test.commandQueue.put((codes.getReady,None))
        if x == "2":
            test.commandQueue.put((codes.start,None))
        if x== "3":
            test.commandQueue.put((codes.stop,None))
        if x== "4":
            test.commandQueue.put((codes.shutdown,None))
        if x== "5":
            test.commandQueue.put((codes.abort,None))
