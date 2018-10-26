#!/usr/bin/python3
import threading
import copy

import sqlite3
from DataObjects import DataObjectCollection, DataObject, detectorDataObject, partitionDataObject, globalSystemDataObject, mappingDataObject
class DataBaseWrapper:
    """Handler for the ECS Database"""
    connection = None

    def __init__(self,logfunction):
        self.connection = sqlite3.connect("ECS_database.db")
        self.log = logfunction

    def close(self):
        """closes the database Connection"""
        self.connection.close()

    def getAllDetectors(self):
        """Get All Detectors in Detector Table; returns empty DataObjectCollection if there are now Detectors"""
        c = self.connection.cursor()
        try:
            c.execute("SELECT * FROM Detector")
            res = c.fetchall()
            return DataObjectCollection(res,detectorDataObject)
        except Exception as e:
            self.log("error getting detectors: %s" % str(e),True)
            return e

    def getDetector(self,id):
        """get Detector with given id; returns ErrorCode if it does not exist"""
        c = self.connection.cursor()
        val = (id,)
        try:
            res = c.execute("SELECT * FROM Detector WHERE id = ?", val).fetchone()
            if not res:
                return codes.idUnknown
            return detectorDataObject(res)
        except Exception as e:
            self.log("error getting detector: %s %s" % (str(id),str(e)),True)
            return e

    def getAllUnmappedDetectors(self):
        """gets all Detectors which are currently unmmaped"""
        c = self.connection.cursor()
        try:
            res = c.execute("SELECT * FROM Detector Where Detector.id not in (select DetectorId From Mapping)").fetchall()
            return DataObjectCollection(res,detectorDataObject)
        except Exception as e:
            self.log("error getting unmapped detectors: %s" % str(e),True)
            return e

    def addDetector(self,dataObject):
        """add a Detector to Database;accepts json String or DataObject"""
        if not isinstance(dataObject,detectorDataObject):
            dataObject = detectorDataObject(json.loads(dataObject))
        c = self.connection.cursor()
        try:
            c.execute("INSERT INTO Detector VALUES (?,?,?,?,?)", dataObject.asArray())
            self.connection.commit()
            return codes.ok
        except Exception as e:
            self.log("error inserting values into Detector Table: %s" % str(e),True)
            self.connection.rollback()
            return e


    def removeDetector(self,id):
        """delete a Detector from Database"""
        c = self.connection.cursor()
        val = (id,)
        try:
            c.execute("DELETE FROM Detector WHERE id = ?", val)
            self.connection.commit()
            return codes.ok
        except Exception as e:
            self.log("error removing values from Detector Table: %s" % str(e),True)
            return e

    def getPartition(self,id):
        """Get Partition with given id from Database; returns None if it does not exist"""
        c = self.connection.cursor()
        val = (id,)
        try:
            res = c.execute("SELECT * FROM Partition WHERE id = ?", val).fetchone()
            if not res:
                return codes.idUnknown
            return partitionDataObject(res)
        except Exception as e:
            self.log("error getting partition %s: %s" % (str(id),str(e)),True)
            return e

    def getPartitionForDetector(self,id):
        """gets the Partition of a Detector; returns DataObject or ErrorCode"""
        c = self.connection.cursor()
        val = (id,)
        try:
            res = c.execute("SELECT * FROM Partition WHERE Partition.id IN (SELECT PartitionId FROM (Mapping JOIN Partition ON Mapping.PartitionId = Partition.id) WHERE DetectorId = ?)", val).fetchone()
            if not res:
                return codes.idUnknown
            return partitionDataObject(res)
        except Exception as e:
            self.log("error getting partition for Detector %s: %s" % (str(id),str(e)),True)
            return e

    def getAllPartitions(self):
        """Get All Detectors in Detector Table"""
        c = self.connection.cursor()
        try:
            c.execute("SELECT * FROM Partition")
            res = c.fetchall()
            return DataObjectCollection(res, partitionDataObject)
        except Exception as e:
            self.log("error getting all partitions: %s" % str(e),True)
            return e

    def getDetectorsForPartition(self,pcaId):
        """get all Mapped Detectors for a given PCA Id"""
        c = self.connection.cursor()
        val = (pcaId,)
        try:
            c.execute("SELECT * From Detector WHERE Detector.id in (SELECT d.id FROM Detector d JOIN Mapping m ON d.id = m.DetectorId WHERE PartitionId=?)",val)
            res = c.fetchall()
            return DataObjectCollection(res, detectorDataObject)
        except Exception as e:
            self.log("error getting all detectors for Partition %s: %s" % str(pcaId), str(e),True)
            return e

    def addPartition(self,dataObject):
        """create new Partition"""
        c = self.connection.cursor()
        data = dataObject.asArray()
        try:
            c.execute("INSERT INTO Partition VALUES (?,?,?,?,?,?,?,?)", data)
            self.connection.commit()
            return codes.ok
        except Exception as e:
            self.log("error inserting values into Partition Table: %s" % str(e),True)
            self.connection.rollback()
            return e

    def removePartition(self,id):
        """delete a Partition with given id"""
        c = self.connection.cursor()
        val = (id,)
        try:
            c.execute("DELETE FROM Partition WHERE id = ?", val)
            #Free the Detectors
            c.execute("DELETE FROM Mapping WHERE PartitionId = ?", val)
            self.connection.commit()
            return codes.ok
        except Exception as e:
            self.connection.rollback()
            self.log("error removing values from Detector Table: %s" % str(e),True)
            return e

    def getDetectorMapping(self):
        """get entire PCA Detector Mapping Table"""
        c = self.connection.cursor()
        try:
            c.execute("SELECT * From Mapping")
            res = c.fetchall()
            return DataObjectCollection(res, mappingDataObject)
        except Exception as e:
            self.log("error getting all detectors for Partition %s: %s" % str(pcaId), str(e),True)
            return e

    def mapDetectorToPCA(self,detId,pcaId):
        """map a Detector to a Partition"""
        c = self.connection.cursor()
        vals = (detId,pcaId)
        try:
            c.execute("INSERT INTO Mapping VALUES (?,?)", vals)
            self.connection.commit()
            return codes.ok
        except Exception as e:
            self.connection.rollback()
            self.log("error mapping %s to %s: %s" % (str(detId),str(pcaId),str(e)),True)
            return e

    def remapDetector(self,detId,newPcaId,oldPcaID):
        """assign Detector to a different Partition"""
        c = self.connection.cursor()
        vals = (detId,newPcaId)
        try:
            c.execute("DELETE FROM Mapping WHERE DetectorId = ?", (detId))
            c.execute("INSERT INTO Mapping VALUES (?,?)", vals)
            self.connection.commit()
            return codes.ok
        except Exception as e:
            self.connection.rollback()
            self.log("error remapping %s from %s to %s: %s" % (str(detId),str(oldPcaID),str(newPcaId),str(e)),True)
            return e

    def unmapDetectorFromPCA(self,detId):
        """unmap a Detector from a Partition"""
        c = self.connection.cursor()
        val = (detId,)
        try:
            c.execute("DELETE FROM Mapping WHERE DetectorId = ?", val)
            self.connection.commit()
            return codes.ok
        except Exception as e:
            self.connection.rollback()
            self.log("error unmapping %s: %s " % (str(detId),str(e)),True)
            return e

    def usedPortsForAddress(self,address):
        """get all used Ports for an Ip-Address returns List of Ports or ErrorCode """
        c = self.connection.cursor()
        val = (address,)
        try:
            ports = []
            c.execute("SELECT Port,PingPort FROM Detector WHERE address=?",val)
            ret = c.fetchall()
            for row in ret:
                for val in row:
                    ports.append(val)
            c.execute("SELECT portPublish,portLog,portUpdates,portCurrentState,portCommand FROM Partition WHERE address=? ",val)
            ret = c.fetchall()
            for row in ret:
                for val in row:
                    ports.append(val)
            return ports
        except Exception as e:
            self.log("error getting Ports for Address %s: %s " % (address,str(e)),True)
            return e

    def getDCSInfo(self):
        """Get DCS Information"""
        c = self.connection.cursor()
        try:
            res = c.execute("SELECT * From GlobalSystems Where id='DCS'").fetchone()
            if not res:
                return codes.idUnknown
            return globalSystemDataObject(res)
        except Exception as e:
            self.log("error getting DCS Info: %s" % str(e),True)
            return e

    def getGlobalSystem(self,id):
        """get global System for Id"""
        c = self.connection.cursor()
        val = (id,)
        try:
            res = c.execute("SELECT * From GlobalSystems Where id=?",val).fetchone()
            if not res:
                return codes.idUnknown
            return globalSystemDataObject(res)
        except Exception as e:
            self.log("error getting DCS Info: %s" % str(e),True)
            return e


import zmq
import logging
import threading
from ECSCodes import ECSCodes
codes = ECSCodes()
import struct
import json
from multiprocessing import Queue
import time
import ECS_tools
from datetime import datetime
import paramiko
#import DataObjects
from  UnmappedDetectorController import UnmappedDetectorController

from channels.layers import get_channel_layer
from GUI.models import Question, Choice, pcaModel
from django.conf import settings
from collections import deque
from django.utils import timezone
from DataObjects import DataObjectCollection, detectorDataObject, partitionDataObject, stateObject
import asyncio
import signal
from states import PCAStates
PCAStates = PCAStates()

class ECS:
    """The Experiment Control System"""
    def __init__(self):
        self.database = DataBaseWrapper(self.log)
        self.partitions = ECS_tools.MapWrapper()
        partitions = self.database.getAllPartitions()
        for p in partitions:
            self.partitions[p.id] = p
        self.disconnectedDetectors = ECS_tools.MapWrapper()
        self.stateMap = ECS_tools.MapWrapper()
        self.logQueue = deque(maxlen=settings.BUFFERED_LOG_ENTRIES)
        #signal.signal(signal.SIGTERM, self.terminateECS)

        self.receive_timeout = settings.TIMEOUT
        self.pingInterval = settings.PINGINTERVAL
        self.pingTimeout = settings.PINGTIMEOUT
        self.pathToPCACodeFile = settings.PATH_TO_PROJECT
        self.pathToDetectorCodeFile = settings.PATH_TO_PROJECT
        self.pathToGlobalSystemFile = settings.PATH_TO_PROJECT
        self.DetectorCodeFileName = settings.DETECTOR_CODEFILE_NAME
        self.PCACodeFileName = settings.PCA_CODEFILE_NAME
        self.globalSystemFileName = settings.GLOBALSYSTEM_CODE_FILE
        self.checkIfRunningScript = settings.CHECK_IF_RUNNING_SCRIPT
        self.virtenvFile =  settings.PYTHON_VIRTENV_ACTIVATE_FILE

        self.zmqContext = zmq.Context()
        self.zmqContext.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        self.zmqContext.setsockopt(zmq.LINGER,0)

        self.zmqContextNoTimeout = zmq.Context()
        self.zmqContextNoTimeout.setsockopt(zmq.LINGER,0)

        #socket for receiving requests from WebUI
        self.replySocket = self.zmqContextNoTimeout.socket(zmq.REP)
        self.replySocket.bind("tcp://*:%s" % settings.ECS_REQUEST_PORT)

        #log publish socket
        self.socketLogPublish = self.zmqContext.socket(zmq.PUB)
        self.socketLogPublish.bind("tcp://*:%s" % settings.ECS_LOG_PORT)

        #init logger
        self.logfile = settings.LOG_PATH_ECS
        debugMode = settings.DEBUG
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
        #disable info logging for paramiko
        logging.getLogger("paramiko").setLevel(logging.WARNING)

        #set console log to info level if in debug mode
        if debugMode:
            logging.getLogger().handlers[1].setLevel(logging.INFO)
        else:
            logging.getLogger().handlers[1].setLevel(logging.CRITICAL)

        #Information for UnmappedDetectorController
        data = {
            "id" : "unmapped",
            "address" : settings.ECS_ADDRESS,
            "portPublish" : settings.UNUSED_DETECTORS_PUBLISH_PORT,
            "portLog" : "-1",
            "portUpdates" : settings.UNUSED_DETECTORS_UPDATES_PORT,
            "portCurrentState" : settings.UNUSED_DETECTORS_CURRENT_STATE_PORT,
            "portSingleRequest" : "-1",
            "portCommand" : "-1",

        }
        self.unmappedDetectorControllerData = partitionDataObject(data)

        systemList = ["TFC","DCS","QA","FLES"]
        res = []
        #get info from database
        for s in systemList:
            res.append(self.database.getGlobalSystem(s))

        #Store GlobalSystem Information
        tfcData, dcsData, qaData, flesData = res
        self.globalSystems = {}
        self.globalSystems["TFC"] = tfcData
        self.globalSystems["DCS"] = dcsData
        self.globalSystems["QA"] = qaData
        self.globalSystems["FLES"] = flesData

        t = threading.Thread(name="requestHandler", target=self.waitForRequests)
        t.start()

        #start PCA clients via ssh
        for p in self.partitions:
            ret = self.startClient(p)

        #clear database from Previous runs
        pcaModel.objects.all().delete()

        self.pcaHandlers = {}
        #create Handlers for PCAs
        for p in self.partitions:
            self.pcaHandlers[p.id] = PCAHandler(p,self.log,self.globalSystems)
            #add database object for storing user permissions
            pcaModel.objects.create(id=p.id,permissionTimestamp=timezone.now())
        #user Permissions ecs
        pcaModel.objects.create(id="ecs",permissionTimestamp=timezone.now())

        self.terminate = False

        #create Controller for Unmapped Detectors
        self.unmappedStateTable = ECS_tools.MapWrapper()
        unmappedDetectors = self.database.getAllUnmappedDetectors()
        self.unmappedDetectorController = UnmappedDetectorController(unmappedDetectors,self.unmappedDetectorControllerData.portPublish,self.unmappedDetectorControllerData.portUpdates,self.unmappedDetectorControllerData.portCurrentState,self.log)

        #todo obsolete?
        t = threading.Thread(name="consistencyCheckThread", target=self.consistencyCheckThread)
        #t.start()

    def getPCAHandler(self,id):
        if id in self.pcaHandlers:
            return self.pcaHandlers[id]
        else:
            return None

    def consistencyCheckThread(self):
        """checks wether states and Detector Assignment is Consistend between all Clients"""
        while True:
            time.sleep(5)
            if self.terminate:
                break
            self.checkSystemConsistency()

    def checkIfRunning(self,clientObject):
        """check if client is Running"""
        if isinstance(clientObject,detectorDataObject):
            path = self.pathToDetectorCodeFile
        elif isinstance(clientObject,globalSystemDataObject):
            path = self.pathToGlobalSystemFile
        elif isinstance(clientObject,partitionDataObject):
            path = self.pathToPCACodeFile
        else:
            raise Exception("Expected detector or partition Object but got %s" % type(clientObject))
        id = clientObject.id
        address = clientObject.address
        fileName = self.checkIfRunningScript
        ssh = None
        try:
            ssh = paramiko.SSHClient()
            ssh.load_system_host_keys()
            #login on pca computer needs a rsa key
            ssh.connect(address)
            #python script returns -1 if not running or the pid otherwise
            stdin, stdout, stderr = ssh.exec_command("pid=$(cd %s;source %s; python %s %s); echo $pid" % (path,self.virtenvFile,fileName,id))
            pid = stdout.readline()
            pid = pid.strip()
            ssh.close()
            if pid == "-1":
                return False
            return pid
        except Exception as e:
            if ssh:
                ssh.close()
            self.log("Exception executing ssh command: %s" % str(e))
            return False

    def startClient(self,clientObject):
        """starts a PCA or Detector client via SSH returns the process Id on success"""
        if isinstance(clientObject,detectorDataObject):
            path = self.pathToDetectorCodeFile
            fileName = self.DetectorCodeFileName
        elif isinstance(clientObject,globalSystemDataObject):
            path = self.pathToGlobalSystemFile
            fileName = self.globalSystemFileName
        elif isinstance(clientObject,partitionDataObject):
            path = self.pathToPCACodeFile
            fileName = self.PCACodeFileName
        else:
            raise Exception("Expected detector, partition or globalSystem Object but got %s" % type(clientObject))
        id = clientObject.id
        address = clientObject.address
        ssh = None
        try:
            pid = self.checkIfRunning(clientObject)
            if not pid:
                ssh = paramiko.SSHClient()
                ssh.load_system_host_keys()
                #login on pca computer needs a rsa key
                ssh.connect(address)
                #start Client and get its pid
                stdin, stdout, stderr = ssh.exec_command("cd %s;source %s;python %s %s > /dev/null & echo $!" % (path,self.virtenvFile,fileName,id))
                pid = stdout.readline()
                pid = pid.strip()
                ssh.close()
            else:
                self.log("Detector Client %s is already Running with PID %s" % (id,pid))
            return pid
        except Exception as e:
            if ssh:
                ssh.close()
            self.log("Exception executing ssh command: %s" % str(e))
            return False

    def stopClient(self,clientObject):
        """kills a PCA or Detector Client via SSH"""
        if not (isinstance(clientObject,detectorDataObject) or isinstance(clientObject,partitionDataObject) or isinstance(clientObject,globalSystemDataObject)):
            raise Exception("Expected detector, partition or globalSystem Object but got %s" % type(clientObject))
        id = clientObject.id
        address = clientObject.address
        pid = self.checkIfRunning(clientObject)
        if not pid:
            self.log("tried to stop %s Client but it wasn't running" % id)
            return True
        ssh = None
        try:
            ssh = paramiko.SSHClient()
            ssh.load_system_host_keys()
            ssh.connect(address)
            stdin, stdout, stderr = ssh.exec_command("kill %s" % (pid,))
            returnValue = stdout.channel.recv_exit_status()
            ssh.close()
            if returnValue == 0:
                return True
            else:
                self.log("error stopping Client for %s" % id)
                return False
        except Exception as e:
            if ssh:
                ssh.close()
            self.log("Exception executing ssh command: %s" % str(e))
            return False

    def createPartition(self,partition):
        """ Create a new Partition and start it's PCA Client"""
        db = DataBaseWrapper(self.log)
        ret = db.addPartition(partition)
        db.close()
        if ret == codes.ok:
            #inform GlobalSystems
            informedSystems = []
            for gsID in self.globalSystems:
                try:
                    gs = self.globalSystems[gsID]
                    requestSocket = self.zmqContext.socket(zmq.REQ)
                    requestSocket.connect("tcp://%s:%s"  % (gs.address,gs.portCommand))
                    requestSocket.send_multipart([codes.addPartition,partition.asJsonString().encode()])
                    ret = requestSocket.recv()
                    if ret != codes.ok:
                        raise Exception("Global System returned ErrorCode")
                    informedSystems.append(gsID)
                except Exception as e:
                    #start rollback(tell all informed Global Systems to remove the new partition)
                    db = DataBaseWrapper(self.log)
                    ret = db.removePartition(partition.id)
                    db.close()
                    requestSocket.close()
                    for gsIDRolback in informedSystems:
                        gs = self.globalSystems[gsIDRolback]
                        requestSocket = self.zmqContext.socket(zmq.REQ)
                        requestSocket.connect("tcp://%s:%s"  % (gs.address,gs.portCommand))
                        requestSocket.send_multipart([codes.deletePartition,partition.id.encode()])
                        ret = requestSocket.recv()
                        if ret != codes.ok:
                            raise Exception("Global System returned ErrorCode during rollback")
                    if isinstance(e,zmq.Again):
                        self.log("timeout informing Global System %s" % (gsID),True)
                        return "timeout informing Global System %s" % (gsID)
                    else:
                        self.log("error informing Global System %s: %s" % (gsID),str(e),True)
                        return "error informing Global System %s: %s " % (gsID,str(e))
                finally:
                    requestSocket.close()

            #start PCA if Not Running
            pid = self.checkIfRunning(partition)
            if not pid:
                pid = self.startClient(partition)
            if not pid:
                self.log("PCA Client for %s could not be startet" % partition.id,True)
            #connect to pca
            self.partitions[partition.id] = partition
            self.pcaHandlers[partition.id] = PCAHandler(partition,self.log,self.globalSystems)
            #add database object for storing user permissions
            pcaModel.objects.create(id=partition.id,permissionTimestamp=timezone.now())
            return True
        else:
            return str(ret)

    def deletePartition(self,pcaId,forceDelete=False):
        """delete a Partition on stop it's client"""
        try:
            db = DataBaseWrapper(self.log)
            partition = db.getPartition(pcaId)
            if isinstance(partition,Exception):
                return str(partition)
            elif partition == codes.idUnknown:
                return "Partition with id %s not found" % pcaId
            detectors = db.getDetectorsForPartition(pcaId)
            if isinstance(detectors,Exception):
                return str(detectors)

            #check if there are Detectors still assigned to the Partition
            if len(detectors.asDictionary()) > 0:
                raise Exception("Can not delete because there are still Detectors assigned to Partition")
            ret = db.removePartition(pcaId)
            if isinstance(ret,Exception):
                return str(ret)

            #inform Global Systems
            informedSystems=[]
            for gsID in self.globalSystems:
                try:
                    gs = self.globalSystems[gsID]
                    requestSocket = self.zmqContext.socket(zmq.REQ)
                    requestSocket.connect("tcp://%s:%s"  % (gs.address,gs.portCommand))
                    requestSocket.send_multipart([codes.deletePartition,partition.id.encode()])
                    ret = requestSocket.recv()
                    if ret != codes.ok:
                        raise Exception("Global System returned ErrorCode")
                    informedSystems.append(gsID)
                except Exception as e:
                    #start rollback
                    db.addPartition(partition)
                    db.close()
                    requestSocket.close()
                    for gsIDRolback in informedSystems:
                        gs = self.globalSystems[gsIDRolback]
                        requestSocket = self.zmqContext.socket(zmq.REQ)
                        requestSocket.connect("tcp://%s:%s"  % (gs.address,gs.portCommand))
                        requestSocket.send_multipart([codes.addPartition,partition.asJsonString().encode()])
                        ret = requestSocket.recv()
                        if ret != codes.ok:
                            raise Exception("Global System returned ErrorCode during rollback")
                    if isinstance(e,zmq.Again):
                        self.log("timeout informing Global System %s" % (gsID),True)
                        if not forceDelete:
                            return "timeout informing Global System %s" % (gsID)
                    else:
                        self.log("error informing Global System %s: %s" % (gsID),str(e),True)
                        if not forceDelete:
                            return "error informing Global System %s: %s " % (gsID,str(e))
                finally:
                    requestSocket.close()
            #try to stop pca Client
            self.stopClient(partition)
            #remove from Maps
            del self.partitions[partition.id]
            #terminate pcaHandler
            self.pcaHandlers[pcaId].terminatePCAHandler()
            del self.pcaHandlers[pcaId]
            #delete PCA Permissions
            pcaModel.objects.get(id=pcaId).delete()

            return True
        except Exception as e:
            self.log("Error Deleting Partition: %s" % str(e))
            return str(e)
        finally:
            db.close()

    def createDetector(self,dataObject):
        """create a new Detector"""
        db = DataBaseWrapper(self.log)
        dbChanged = False
        try:
            ret = db.addDetector(dataObject)
            if ret == codes.ok:
                dbChanged = True
                if self.unmappedDetectorController.checkIfTypeIsKnown(dataObject):
                    ret = self.unmappedDetectorController.addDetector(dataObject)
                    return True
                else:
                    raise Exception("Detector Type is unknown")
            else:
                if isinstance(ret,Exception):
                    return str(ret)
                else:
                    return "Database error"
        except Exception as e:
            if dbChanged:
                db.removeDetector(dataObject.id)
            raise e
            return str(e)
        finally:
            db.close()

    def deleteDetector(self,detId,forceDelete=False):
        """deletes Detector enirely from System trys to shutdown the Detector;
         with forceDelete User has the possibillity to delete from Databse without shutting down the detector;
         only unmapped Detectors can be deleted"""
        db = DataBaseWrapper(self.log)
        detector = db.getDetector(detId)
        db.close()
        if isinstance(detector,Exception):
            return str(detector)
        if detector == codes.idUnknown:
            return "Detector Id is unknown"

        if not forceDelete:
            #add to UnmappedDetectorController
            if not self.unmappedDetectorController.isDetectorConnected(detector.id):
                return "Detector %s is not connected" % detector.id
            if not self.unmappedDetectorController.abortDetector(detector.id):
                self.log("Detector %s could not be shutdown" % (detector.id))
                return Exception("Detector %s could not be shutdown" % (detector.id))

        self.unmappedDetectorController.removeDetector(detector.id)
        #delete from Database
        db = DataBaseWrapper(self.log)
        ret = db.removeDetector(detector.id)
        db.close()
        if ret != codes.ok:
            return "Error removing Detector %s from databse" % detector.id
        return True

    def getDetectorsForPartition(self,pcaId):
        db = DataBaseWrapper(self.log)
        detectors = db.getDetectorsForPartition(pcaId)
        db.close()
        return detectors

    def getDetector(self,id):
        """get's Detector Data from the database"""
        db = DataBaseWrapper(self.log)
        detector = db.getDetector(id)
        db.close()
        return detector

    def getUnmappedDetectors(self):
        db = DataBaseWrapper(self.log)
        ret = db.getAllUnmappedDetectors()
        db.close()
        if isinstance(ret,Exception):
            return str(ret)
        else:
            return ret

    def moveDetector(self,detectorId,partitionId,forceMove=False):
        """moves a Detector between Partitions"""
        removed = False
        added = False
        dbChanged = False
        #is not assigned to any partition(unmapped)
        unused = False
        informedSystems=[]

        skipUnmap = False
        skipAdd = False

        db = DataBaseWrapper(self.log)
        oldPartition = db.getPartitionForDetector(detectorId)
        if oldPartition == codes.idUnknown:
            unused = True
        else:
            if not self.pcaHandlers[oldPartition.id].PCAConnection:
                #skip informing PCA if its not connected and forceMove is True
                if forceMove:
                    skipUnmap = True
                else:
                    db.close()
                    return "Partition %s is not connected" % oldPartition.id
        if partitionId == "unmapped":
            #detector will be unmapped
            newPartition = False
        else:
            if not self.pcaHandlers[partitionId].PCAConnection:
                if forceMove:
                    skipAdd = True
                else:
                    db.close()
                    return "Partition %s is not connected" % partitionId
            newPartition = self.partitions[partitionId]

        try:
            detector = db.getDetector(detectorId)
            #change Database
            if unused:
                if db.mapDetectorToPCA(detectorId,newPartition.id) != codes.ok:
                    raise Exception("Error during changing Database")
            elif not newPartition:
                if db.unmapDetectorFromPCA(detectorId) != codes.ok:
                    raise Exception("Error during changing Database")
            else:
                if db.remapDetector(detectorId,newPartition.id,oldPartition.id) != codes.ok:
                    raise Exception("Error during changing Database")
            print("db done")
            dbChanged = True

            #lock partitions(PCAs don't accept commands while locked)
            partitionsToLock = []
            lockedPartitions = []
            if not unused:
                partitionsToLock.append(oldPartition)
            if newPartition:
                partitionsToLock.append(newPartition)
            for p in partitionsToLock:
                try:
                    requestSocket = self.zmqContext.socket(zmq.REQ)
                    requestSocket.connect("tcp://%s:%s"  % (p.address,p.portCommand))
                    requestSocket.send_multipart([codes.lock])
                    ret = requestSocket.recv()
                    if ret != codes.ok:
                        self.log("%s returned error for locking Partition" % (p.id),True)
                        raise Exception("%s returned error for locking Partition" % (p.id))
                    lockedPartitions.append(p)
                except zmq.Again:
                    self.log("timeout locking Partition %s" % (p.id),True)
                    raise Exception("timeout locking Partition %s" % (p.id))
                except Exception as e:
                    self.log("error locking Partition %s: %s " % (p.id,str(e)),True)
                    raise Exception("error locking Partition %s: %s " % (p.id,str(e)))
                finally:
                    requestSocket.close()

            if not unused:
                if not skipUnmap:
                    #remove from Old Partition
                    try:
                        requestSocket = self.zmqContext.socket(zmq.REQ)
                        requestSocket.connect("tcp://%s:%s"  % (oldPartition.address,oldPartition.portCommand))
                        requestSocket.send_multipart([codes.removeDetector,detectorId.encode()])
                        ret = requestSocket.recv()
                        if ret == codes.busy:
                            self.log("%s is not in Idle State" % (oldPartition.id),True)
                            raise Exception("%s is not in Idle State" % (oldPartition.id))
                        elif ret != codes.ok:
                            self.log("%s returned error for removing Detector" % (oldPartition.id),True)
                            raise Exception("%s returned error for removing Detector" % (oldPartition.id))
                    except zmq.Again:
                        self.log("timeout removing Detector from %s" % (oldPartition.id),True)
                        raise Exception("timeout removing Detector from %s" % (oldPartition.id))
                    except Exception as e:
                        self.log("error removing Detector from %s: %s " % (oldPartition.id,str(e)),True)
                        raise Exception("error removing Detector from %s: %s " % (oldPartition.id,str(e)))
                    finally:
                        requestSocket.close()
            else:
                #remove from Unused Detectors
                self.unmappedDetectorController.removeDetector(detectorId)
            removed = True
            print("removed")

            if newPartition:
                if not skipAdd:
                    try:
                        #add to new Partition
                        requestSocket = self.zmqContext.socket(zmq.REQ)
                        requestSocket.connect("tcp://%s:%s"  % (newPartition.address,newPartition.portCommand))
                        requestSocket.send_multipart([codes.addDetector,detector.asJsonString().encode()])
                        ret = requestSocket.recv()
                        if ret == codes.busy:
                            self.log("%s is not in Idle State" % (newPartition.id),True)
                            raise Exception("%s is not in Idle State" % (newPartition.id))
                        elif ret != codes.ok:
                            self.log("%s returned error for adding Detector" % (newPartition.id),True)
                            raise Exception("%s returned error for adding Detector" % (newPartition.id))
                    except zmq.Again:
                        self.log("timeout adding Detector to %s" % (newPartition.id),True)
                        raise Exception("timeout adding Detector to %s" % (newPartition.id))
                    except Exception as e:
                        self.log("error adding Detector to %s: %s " % (newPartition.id,str(e)),True)
                        raise Exception("error adding Detector to %s: %s " % (newPartition.id,str(e)))
                    finally:
                        requestSocket.close()
            else:
                #add to unused detectors
                self.unmappedDetectorController.addDetector(detector)
            added = True

            #inform GlobalSystems
            for gsID in self.globalSystems:
                try:
                    gs = self.globalSystems[gsID]
                    requestSocket = self.zmqContext.socket(zmq.REQ)
                    requestSocket.connect("tcp://%s:%s"  % (gs.address,gs.portCommand))
                    if newPartition:
                        requestSocket.send_multipart([codes.remapDetector,newPartition.id.encode(),detector.id.encode()])
                    else:
                        requestSocket.send_multipart([codes.remapDetector,codes.removed,detector.id.encode()])
                    ret = requestSocket.recv()
                    if ret != codes.ok:
                        raise Exception("Global System returned ErrorCode")
                    informedSystems.append(gsID)
                except zmq.Again:
                    self.log("timeout informing Global System %s" % (gsID),True)
                    raise Exception("timeout informing Global System %s" % (gsID))
                except Exception as e:
                    self.log("error informing Global System %s: %s " % (gsID,str(e)),True)
                    raise Exception("error informing Global System %s: %s " % (gsID,str(e)))
                finally:
                    requestSocket.close()

            #inform DetectorController
            requestSocket = self.zmqContext.socket(zmq.REQ)
            requestSocket.connect("tcp://%s:%s"  % (detector.address,detector.portCommand))
            if newPartition:
                requestSocket.send_multipart([codes.detectorChangePartition,newPartition.asJsonString().encode()])
            else:
                requestSocket.send_multipart([codes.detectorChangePartition,self.unmappedDetectorControllerData.asJsonString().encode()])
            try:
                ret = requestSocket.recv()
                if ret != codes.ok:
                    self.log("%s returned error for changing PCA" % (detector.id),True)
                    raise Exception("%s returned error for changing PCA" % (detector.id,))
            except zmq.Again:
                self.log("timeout informing Detector %s" % (detector.id),True)
                if not forceMove:
                    raise Exception("timeout informing Detector %s" % (detector.id))
            except Exception as e:
                self.log("error changing Detector %s PCA: %s " % (detector.id),str(e),True)
                raise Exception("error changing Detector %s PCA: %s " % (detector.id),str(e))
            finally:
                requestSocket.close()
            return True
        except Exception as e:
            self.log("error during remapping:%s ;starting rollback for remapping Detector" % str(e),True)
            #rollback
            try:
                if dbChanged:
                    if unused:
                        if db.unmapDetectorFromPCA(detectorId) != codes.ok:
                            raise Exception("Error during changing Database")
                    elif not newPartition:
                        if db.mapDetectorToPCA(detectorId,oldPartition.id) != codes.ok:
                            raise Exception("Error during changing Database")
                    else:
                        if db.remapDetector(detectorId,oldPartition.id,newPartition.id) != codes.ok:
                            raise Exception("Error during changing Database")

                #inform GlobalSystems
                for gsID in informedSystems:
                    try:
                        gs = self.globalSystems[gsID]
                        requestSocket = self.zmqContext.socket(zmq.REQ)
                        requestSocket.connect("tcp://%s:%s"  % (gs.address,gs.portCommand))
                        if not unused:
                            requestSocket.send_multipart([codes.remapDetector,oldPartition.id.encode(),detector.id.encode()])
                        else:
                            requestSocket.send_multipart([codes.remapDetector,codes.removed,detector.id.encode()])
                        ret = requestSocket.recv()
                        if ret != codes.ok:
                            raise Exception("Global System returned ErrorCode")
                    except zmq.Again:
                        self.log("timeout informing Global System %s" % (gsID),True)
                        raise Exception("timeout informing Global System %s" % (gsID))
                    except Exception as e:
                        self.log("error informing Global System %s: %s " % (gsID,str(e)),True)
                        raise Exception("error informing Global System %s: %s " % (gsID,str(e)))
                    finally:
                        requestSocket.close()

                if removed:
                    if not unused:
                        requestSocket = self.zmqContext.socket(zmq.REQ)
                        requestSocket.connect("tcp://%s:%s"  % (oldPartition.address,oldPartition.portCommand))
                        requestSocket.send_multipart([codes.addDetector,detector.asJsonString().encode()])
                        try:
                            ret = requestSocket.recv()
                            requestSocket.close()
                        except zmq.Again as e:
                            self.log("timeout adding Detector to %s" % (oldPartition.id),True)
                            requestSocket.close()
                            raise Exception("timeout during adding for pca:%s" % oldPartition.id )
                        except Exception as e:
                            self.log("error adding Detector to %s: %s " % (oldPartition.id,str(e)),True)
                            requestSocket.close()
                            raise Exception("Error during adding :%s" % str(e) )
                        if ret == codes.busy:
                            self.log("%s is not in Idle State" % (oldPartition.id),True)
                            raise Exception("%s is not in Idle State" % (oldPartition.id))
                        elif ret != codes.ok:
                            self.log("%s returned error for adding Detector" % (oldPartition.id),True)
                            requestSocket.close()
                            raise Exception("pca returned error code during adding Detector")
                        requestSocket.close()
                    else:
                        #add to unused detectors
                        self.unmappedDetectorController.addDetector(detector)
                if added:
                    if newPartition:
                        try:
                            requestSocket = self.zmqContext.socket(zmq.REQ)
                            requestSocket.connect("tcp://%s:%s"  % (newPartition.address,newPartition.portCommand))
                            requestSocket.send_multipart([codes.removeDetector,detectorId.encode()])
                            ret = requestSocket.recv()
                        except zmq.Again:
                            self.log("timeout removing Detector from %s" % (newPartition.id),True)
                            requestSocket.close()
                            raise Exception("timeout removing Detector from %s" % (newPartition.id))
                        except Exception:
                            self.log("error removing Detector from %s: %s " % (newPartition.id,str(e)),True)
                            requestSocket.close()
                            raise Exception("error removing Detector from %s: %s " % (newPartition.id,str(e)))
                        finally:
                            requestSocket.close()
                    if ret == codes.busy:
                        self.log("%s is not in Idle State" % (newPartition.id),True)
                        raise Exception("%s is not in Idle State" % (newPartition.id))
                    elif ret != codes.ok:
                            self.log("%s returned error for removing Detector" % (newPartition.id),True)
                            raise Exception("%s returned error for removing Detector" % (oldPartition.id))
                    else:
                        #remove from Unused Detectors
                        self.unmappedDetectorController.removeDetector(detectorId)
                return str(e)
            except Exception as e:
                self.log("Exception during roll back %s" %str(e),True)
                return str(e)
        finally:
            db.close()
            #unlock partitions
            for p in lockedPartitions:
                try:
                    requestSocket = self.zmqContext.socket(zmq.REQ)
                    requestSocket.connect("tcp://%s:%s"  % (p.address,p.portCommand))
                    requestSocket.send_multipart([codes.unlock])
                    ret = requestSocket.recv()
                    if ret != codes.ok:
                        self.log("%s returned error for unlocking Partition" % (p.id),True)
                        raise Exception("%s returned unlocking Partition" % (p.id))
                except zmq.Again:
                    self.log("timeout unlocking Partition %s" % (p.id),True)
                    raise Exception("timeout unlocking Partition %s" % (p.id))
                except Exception as e:
                    self.log("error unlocking Partition %s: %s " % (p.id,str(e)),True)
                    raise Exception("error unlocking Partition %s: %s " % (p.id,str(e)))
                finally:
                    requestSocket.close()


    def waitForRequests(self):
        """waits for client Requests"""
        #SQLite objects created in a thread can only be used in that same thread. So we need a second connection -_-
        db = DataBaseWrapper(self.log)
        while True:
            try:
                m = self.replySocket.recv_multipart()
            except zmq.error.ContextTerminated:
                self.replySocket.close()
                break
            arg = None
            if len(m) == 2:
                code, arg = m
                arg = arg.decode()
            elif len(m) == 1:
                code = m[0]
            else:
                self.log("received malformed request message: %s", str(m),True)
                continue

            def partitionForDetector(arg):
                ret = db.getPartitionForDetector(arg)
                if ret != codes.idUnknown:
                    return ret
                else:
                    return self.unmappedDetectorControllerData


            def switcher(code,arg=None):
                #functions for codes
                dbFunctionDictionary = {
                    codes.pcaAsksForConfig: db.getPartition,
                    codes.detectorAsksForPCA: partitionForDetector,
                    codes.getDetectorForId: db.getDetector,
                    codes.pcaAsksForDetectorList: db.getDetectorsForPartition,
                    codes.getPartitionForId: db.getPartition,
                    codes.getAllPCAs: db.getAllPartitions,
                    codes.getUnmappedDetectors: db.getAllUnmappedDetectors,
                    codes.GlobalSystemAsksForInfo: db.getGlobalSystem,
                    codes.getDetectorMapping: db.getDetectorMapping,
                }
                #returns function for Code or None if the received code is unknown
                f = dbFunctionDictionary.get(code,None)
                try:
                    if not f:
                        self.log("received unknown command",True)
                        self.replySocket.send(codes.unknownCommand)
                        return
                    if arg:
                        ret = f(arg)
                    else:
                        ret = f()
                    #is result a Dataobject?
                    if isinstance(ret,DataObject) or isinstance(ret,DataObjectCollection):
                        #encode Dataobject
                        ret = ret.asJsonString().encode()
                        self.replySocket.send(ret)
                    elif isinstance(ret,Exception):
                        #it's an error message
                        errorMessage = str(ret)
                        ret = { "error": errorMessage }
                        ret = json.dumps(ret).encode()
                        self.replySocket.send(ret)
                    else:
                        #it's just a returncode
                        self.replySocket.send(ret)
                except zmq.error.ContextTerminated:
                    self.replySocket.close()
                    return
            if arg:
                switcher(code,arg)
            else:
                switcher(code)

    def checkPartition(self,partition):
        """checks is Partition has the corrent DetectorList"""
        db = DataBaseWrapper(self.log)
        detectors = db.getDetectorsForPartition(partition.id)
        if isinstance(detectors,Exception):
            db.close()
            raise detectors
        if detectors == codes.idUnknown:
            db.close()
            Raise(Exception("Partition %s is not in database") % partition.id)
        db.close()
        try:
            #for whatever reason this raises a different Exception for ContextTerminated than send or recv
            requestSocket = self.zmqContext.socket(zmq.REQ)
        except zmq.error.ZMQError:
            return
        try:
            requestSocket.connect("tcp://%s:%s"  % (partition.address,partition.portCommand))
            requestSocket.send_multipart([codes.check,detectors.asJsonString().encode()])
            ret = requestSocket.recv()
        except zmq.Again:
            handler = self.getPCAHandler(partition.id)
            handler.handleDisconnection()
        except zmq.error.ContextTerminated:
            pass
        except Exception as e:
            self.log("error checking PCA %s: %s " % (partition.id,str(e)),True)
        finally:
            requestSocket.close()

    def checkDetector(self,detector):
        """check if Detector has the correct Partition"""
        db = DataBaseWrapper(self.log)
        partition = db.getPartitionForDetector(detector.id)
        if isinstance(partition,Exception):
            db.close()
            raise detectors
        if partition == codes.idUnknown:
            partition = self.unmappedDetectorControllerData
        db.close()
        try:
            #for whatever reason this raises a different Exception for ContextTerminated than send or recv
            requestSocket = self.zmqContext.socket(zmq.REQ)
        except zmq.error.ZMQError:
            return
        try:
            requestSocket = self.zmqContext.socket(zmq.REQ)
            requestSocket.connect("tcp://%s:%s"  % (detector.address,detector.portCommand ))
            requestSocket.send_multipart([codes.check,partition.asJsonString().encode()])
            ret = requestSocket.recv()
            if detector.id in self.disconnectedDetectors:
                del self.disconnectedDetectors[detector.id]
        except zmq.Again:
            if detector.id not in self.disconnectedDetectors:
                self.log("timeout checking Detector %s" % (detector.id),True)
                self.disconnectedDetectors[detector.id] = detector.id
        except zmq.error.ContextTerminated:
            pass
        except Exception as e:
            self.log("error checking Detector %s: %s " % (detector.id,str(e)),True)
        finally:
            requestSocket.close()

    def checkSystemConsistency(self):
        """check if Detector assignmet is correct"""
        db = DataBaseWrapper(self.log)
        unmappedDetectors = db.getAllUnmappedDetectors()
        db.close()
        for d in unmappedDetectors:
            if d.id not in self.unmappedDetectorController.detectors:
                self.log("System check: Detector %s should have been in unmapped Detectors" % d.id,True)
                self.unmappedDetectorController.addDetector(d)
        for detId in self.unmappedDetectorController.detectors.keyIterator():
            if detId not in unmappedDetectors.asDictionary():
                self.log("System check: Detector %s should not have been in unmapped Detectors" % detId,True)
                self.unmappedDetectorController.removeDetector(detId)

        for p in self.partitions:
            self.checkPartition(p)

        db = DataBaseWrapper(self.log)
        detectors = db.getAllDetectors()
        db.close()
        for d in detectors:
            self.checkDetector(d)

    def log(self,message,error=False,origin="ecs"):
        """log to file, terminal and Websocket"""
        str=datetime.now().strftime("%Y-%m-%d %H:%M:%S")+":" + message
        try:
            self.socketLogPublish.send(str.encode())
        except:
            self.socketLogPublish.close()
        if error:
            logging.critical(message)
        else:
            logging.info(message)
        """spread log message through websocket(channel)"""
        channel_layer = get_channel_layer()
        message = origin+": "+str
        self.logQueue.append(message)
        async def sendUpdate(recipient,type,message):
            await channel_layer.group_send(
                #group name
                "ecs",
                {
                    #method called in consumer
                    'type': 'logUpdate',
                    'text': message,
                    'origin': origin,
                }
            )
        #python < 3.7
        """loop = asyncio.new_event_loop()
        task = loop.create_task(sendU(type,message))
        result = loop.run_until_complete(task)"""
        #run only exists in python3.7
        asyncio.run(sendUpdate("ecs",type,message))
        """
        async_to_sync(channel_layer.group_send)(
            #group name
            "ecs",
            {
                #method called in consumer
                'type': 'logUpdate',
                'text': message,
                'origin': origin,
            }
        )
        """

    def terminateECS(self):
        """cleanup on shutdown"""
        for p in self.partitions:
            self.stopClient(p)
        self.terminate = True
        #to make get stop Blocking
        self.disconnectedPCAQueue.put(False)
        self.database.close()
        self.socketLogPublish.close()
        self.unmappedDetectorController.terminateContoller()
        self.zmqContext.term()
        self.zmqContextNoTimeout.term()

class PCAHandler:
    """Handler Object for Partition Agents"""
    def __init__(self,partitionInfo,ecsLogfunction,globalSystems):
        self.id = partitionInfo.id
        self.address = partitionInfo.address
        self.portLog = partitionInfo.portLog
        self.portCommand = partitionInfo.portCommand
        self.portPublish = partitionInfo.portPublish
        self.portCurrentState = partitionInfo.portCurrentState
        self.ecsLogfunction = ecsLogfunction
        self.globalSystems = globalSystems

        self.context = zmq.Context()
        self.stateMap = ECS_tools.MapWrapper()
        self.logQueue = deque(maxlen=settings.BUFFERED_LOG_ENTRIES)

        self.PCAConnection = False
        self.receive_timeout = settings.TIMEOUT
        self.pingTimeout = settings.PINGTIMEOUT
        self.pingInterval = settings.PINGINTERVAL

        self.commandSocketAddress = "tcp://%s:%s" % (self.address,self.portCommand)

        #state Change subscription
        self.socketSubscription = self.context.socket(zmq.SUB)
        self.socketSubscription.connect("tcp://%s:%s" % (self.address, self.portPublish))
        #subscribe to everything
        self.socketSubscription.setsockopt(zmq.SUBSCRIBE, b'')

        #logsubscription
        self.socketSubLog = self.context.socket(zmq.SUB)
        self.socketSubLog.connect("tcp://%s:%s" % (self.address,self.portLog))
        self.socketSubLog.setsockopt(zmq.SUBSCRIBE, b'')

        t = threading.Thread(name="updater", target=self.waitForUpdates)
        r = ECS_tools.getStateSnapshot(self.stateMap,partitionInfo.address,partitionInfo.portCurrentState,timeout=self.receive_timeout,pcaid=self.id)
        if r:
            self.PCAConnection = True
        t.start()

        t = threading.Thread(name="logUpdater", target=self.waitForLogUpdates)
        t.start()
        t = threading.Thread(name="heartbeat", target=self.pingHandler)
        t.start()

    def createCommandSocket(self):
        socket = self.context.socket(zmq.REQ)
        socket.connect(self.commandSocketAddress)
        socket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        socket.setsockopt(zmq.LINGER,0)
        return socket

    def pingHandler(self):
        """send heartbeat/ping"""
        socket = self.createCommandSocket()
        while True:
            nextPing = time.time() + self.pingInterval
            try:
                socket.send(codes.ping)
                r = socket.recv()
            except zmq.error.ContextTerminated:
                break
            except zmq.Again:
                self.handleDisconnection()
                #reset Socket
                socket.close()
                socket = self.createCommandSocket()
            except Exception as e:
                self.log("Exception while sending Ping: %s" % str(e))
                socket.close()
                socket = self.createCommandSocket()
            finally:
                nextPing = time.time() + self.pingInterval
            if time.time() > nextPing:
                nextPing = time.time() + self.pingInterval
            else:
                time.sleep(self.pingInterval)

    def sendCommand(self,command,arg=None):
        """send command to pca return True on Success"""
        command = [command]
        if arg:
            command.append(arg.encode())
        commandSocket = self.createCommandSocket()
        commandSocket.send_multipart(command)
        try:
            r = commandSocket.recv()
        except zmq.Again:
            self.handleDisconnection()
            return False
        finally:
            commandSocket.close()
        if r != codes.ok:
            self.log("received error for sending command")
            return False
        return True

    def handleDisconnection(self):
        if self.PCAConnection:
            self.log("PCA %s Connection Lost" % self.id)
        self.PCAConnection = False
        r = ECS_tools.getStateSnapshot(self.stateMap,self.address,self.portCurrentState,timeout=self.receive_timeout,pcaid=self.id)
        if r:
            self.log("PCA %s connected" % self.id)
            self.PCAConnection = True

    def waitForUpdates(self):
        """wait for updates on subscription socket"""
        while True:
            try:
                m = self.socketSubscription.recv_multipart()
            except zmq.error.ContextTerminated:
                self.socketSubscription.close()
                break
            if len(m) != 3:
                print (m)
            else:
                id,sequence,state = m


            id = id.decode()
            sequence = ECS_tools.intFromBytes(sequence)

            if state == codes.reset:
                self.stateMap.reset()
                #reset code for Web Browser
                state = "reset"
            elif state == codes.removed:
                del self.stateMap[id]
                #remove code for Web Browser
                state = "remove"
            else:
                state = json.loads(state.decode())
                self.stateMap[id] = (sequence, stateObject(state))

            isGlobalSystem = id in self.globalSystems
            #send update to WebUI(s)
            jsonWebUpdate = {"id" : id,
                             "state" : state,
                             "sequenceNumber" : sequence,
                             "isGlobalSystem" : isGlobalSystem,
                            }
            if id == self.id and isinstance(state,dict):
                jsonWebUpdate["buttons"] = PCAStates.UIButtonsForState(state["state"])
                print(jsonWebUpdate)
            jsonWebUpdate = json.dumps(jsonWebUpdate)
            self.sendUpdateToWebsockets("update",jsonWebUpdate)

    def log(self,message):
        """spread log message through websocket(channel)"""
        self.logQueue.append(message)
        self.ecsLogfunction(message,origin=self.id)
        self.sendUpdateToWebsockets("logUpdate",message)

    def sendUpdateToWebsockets(self,type,message):
        """send state Updates to the Websocket"""
        channel_layer = get_channel_layer()
        async def sendUpdate(recipient,type,message):
            if recipient != "ecs":
                await channel_layer.group_send(
                    recipient,
                    {
                        #calls method update in the consumer which is registered to channel layer
                        'type': type,
                        #argument(s) with which update is called
                        'text': message,
                    }
                )
            else:
                await channel_layer.group_send(
                    recipient,
                    {
                        #calls method update in the consumer which is registered to channel layer
                        'type': type,
                        #argument(s) with which update is called
                        'text': message,
                        #ecs page needs to know where the update came from
                        'origin': self.id,
                    }
                )
        #python < 3.7
        """loop = asyncio.new_event_loop()
        task = loop.create_task(sendU(type,message))
        result = loop.run_until_complete(task)"""
        #run only exists in python3.7
        asyncio.run(sendUpdate(self.id,type,message))
        if type != "logUpdate":
            #ecs page only needs state Updates
            asyncio.run(sendUpdate("ecs",type,message))

    def waitForLogUpdates(self):
        """wait for new log messages from PCA"""
        while True:
            try:
                m = self.socketSubLog.recv().decode()
            except zmq.error.ContextTerminated:
                self.socketSubLog.close()
                break
            self.log(m)

    def terminatePCAHandler(self):
        """cleanup on shutdown"""
        self.context.term()
        print("%s terminated" % self.id)
