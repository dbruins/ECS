#!/usr/bin/python3
import threading
import copy

import sqlite3
from DataObjects import DataObjectCollection, DataObject, detectorDataObject, partitionDataObject
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
                return ECSCodes.idUnknown
            return detectorDataObject(res)
        except Exception as e:
            self.log("error getting detector: %s %s" % (str(id),str(e)),True)
            return e

    def getAllUnmappedDetetectos(self):
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
            return ECSCodes.ok
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
            return ECSCodes.ok
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
                return ECSCodes.idUnknown
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
                return ECSCodes.idUnknown
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
            return ECSCodes.ok
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
            return ECSCodes.ok
        except Exception as e:
            self.connection.rollback()
            self.log("error removing values from Detector Table: %s" % str(e),True)
            return e

    def mapDetectorToPCA(self,detId,pcaId):
        """map a Detector to a Partition"""
        c = self.connection.cursor()
        vals = (detId,pcaId)
        try:
            c.execute("INSERT INTO Mapping VALUES (?,?)", vals)
            self.connection.commit()
            return ECSCodes.ok
        except Exception as e:
            self.connection.rollback()
            self.log("error mapping %s to %s: %s" % (str(detId),str(pcaId),str(e)),True)
            return e

    def remapDetector(self,detId,newPcaId,oldPcaID):
        c = self.connection.cursor()
        vals = (detId,newPcaId)
        try:
            c.execute("DELETE FROM Mapping WHERE DetectorId = ?", (detId))
            c.execute("INSERT INTO Mapping VALUES (?,?)", vals)
            self.connection.commit()
            return ECSCodes.ok
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
            return ECSCodes.ok
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

import zmq
import logging
import threading
import configparser
import ECSCodes
import struct
import json
from multiprocessing import Queue
import time
import ECS_tools
from datetime import datetime
import paramiko
#import DataObjects
from  UnmappedDetectorController import UnmappedDetectorController

class ECS:
    """The Experiment Control System"""
    def __init__(self):
        self.database = DataBaseWrapper(self.log)
        self.partitions = ECS_tools.MapWrapper()
        partitions = self.database.getAllPartitions()
        for p in partitions:
            self.partitions[p.id] = p
        self.connectedPartitions = ECS_tools.MapWrapper()
        self.disconnectedDetectors = ECS_tools.MapWrapper()
        self.stateMap = ECS_tools.MapWrapper()

        self.disconnectedPCAQueue = Queue()

        config = configparser.ConfigParser()
        config.read("init.cfg")
        conf = config["Default"]

        self.receive_timeout = int(conf["receive_timeout"])
        self.pingInterval = int(conf["pingInterval"])
        self.pingTimeout = int(conf["pingTimeout"])
        self.pathToPCACodeFile = conf["pathToPCACodeFile"]
        self.PCACodeFileName = conf["PCACodeFileName"]
        self.checkIfRunningScript = conf["checkRunningScript"]

        self.zmqContext = zmq.Context()
        self.zmqContext.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        self.zmqContext.setsockopt(zmq.LINGER,0)

        self.zmqContextNoTimeout = zmq.Context()
        self.zmqContextNoTimeout.setsockopt(zmq.LINGER,0)

        #socket for receiving requests from WebUI
        self.replySocket = self.zmqContextNoTimeout.socket(zmq.REP)
        self.replySocket.bind("tcp://*:%s" % conf["ECSRequestPort"])

        #log publish socket
        self.socketLogPublish = self.zmqContext.socket(zmq.PUB)
        self.socketLogPublish.bind("tcp://*:%s" % conf["ECSLogPort"])

        #init logger
        self.logfile = conf["logPathECS"]
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
        #disable info logging for paramiko
        logging.getLogger("paramiko").setLevel(logging.WARNING)

        #set console log to info level if in debug mode
        if debugMode:
            logging.getLogger().handlers[1].setLevel(logging.INFO)
        else:
            logging.getLogger().handlers[1].setLevel(logging.CRITICAL)

        #subscribe to all Partitions
        self.socketSubscription = self.zmqContextNoTimeout.socket(zmq.SUB)
        self.socketSubscription.setsockopt(zmq.SUBSCRIBE, b'')
        for p in self.partitions:
            address = p.address
            port = p.portPublish

            self.socketSubscription.connect("tcp://%s:%i" % (address,port))

        data = {
            "id" : "unmapped",
            "address" : conf["ECSAddress"],
            "portPublish" : conf["ECSPublishUpdatesUnsedDetectors"],
            "portLog" : "-1",
            "portUpdates" : conf["ECSUpdatePortUnsedDetectors"],
            "portCurrentState" : conf["ECSServeStateTableUnsedDetectors"],
            "portSingleRequest" : "-1",
            "portCommand" : "-1",

        }
        self.unmappedDetectorControllerData = partitionDataObject(data)

        t = threading.Thread(name="updater", target=self.waitForUpdates)
        t.start()

        t = threading.Thread(name="requestHandler", target=self.waitForRequests)
        t.start()

        #for storing PCA und Detector Process Ids
        self.clientProcesses = {}
        #start PCA clients via ssh
        for p in self.partitions:
            ret = self.startClient(p)


        #get snapshots from PCAs
        for p in self.partitions:
            id = p.id
            address = p.address
            port = p.portCurrentState

            #if not self.getStateSnapshot(id,address,port):
            if not ECS_tools.getStateSnapshot(self.stateMap,address,port,timeout=self.receive_timeout,pcaid=id):
                self.handleDisconnection(id)
            else:
                self.connectedPartitions[id] = id
        self.reconnectorThread =  threading.Thread(name="reconnectorThread", target=self.reconnector)
        self.terminate = False
        self.reconnectorThread.start()

        t = threading.Thread(name="pingHandler", target=self.pingHandler)
        t.start()

        #create Controller for Unmapped Detectors
        unmappedDetectors = self.database.getAllUnmappedDetetectos()
        self.unmappedDetectorController = UnmappedDetectorController(unmappedDetectors,conf["ECSPublishUpdatesUnsedDetectors"],conf["ECSUpdatePortUnsedDetectors"],conf["ECSServeStateTableUnsedDetectors"],self.log)


        t = threading.Thread(name="consistencyCheckThread", target=self.consistencyCheckThread)
        t.start()

    def consistencyCheckThread(self):
        while True:
            time.sleep(5)
            if self.terminate:
                break
            self.checkSystemConsistency()

    def pingHandler(self):
        """send heartbeat/ping"""
        while True:
            if self.terminate:
                break
            #sequential message processing might scale very badly if there a lot of pca especially if a pca has timeout
            nextPing = time.time() + self.pingInterval
            for id in self.connectedPartitions:
                pca = self.partitions[id]
                try:
                    socket = self.zmqContext.socket(zmq.REQ)
                    socket.connect("tcp://%s:%s" % (pca.address,pca.portCommand))
                    socket.send(ECSCodes.ping)
                    r = socket.recv()
                except zmq.Again:
                    self.handleDisconnection(id)
                except zmq.error.ContextTerminated:
                    pass
                finally:
                    socket.close()
            if time.time() > nextPing:
                continue
            else:
                time.sleep(self.pingInterval)

    def reconnector(self):
        """Thread which trys to reconnect to PCAs"""
        while True:
            pca = self.disconnectedPCAQueue.get()
            if self.terminate:
                break
            if not ECS_tools.getStateSnapshot(self.stateMap,pca.address,pca.portCurrentState,pcaid=pca.id,timeout=self.receive_timeout):
                self.disconnectedPCAQueue.put(pca)
            else:
                self.connectedPartitions[pca.id] = pca.id

    def handleDisconnection(self,id):
        del self.connectedPartitions[id]
        self.log("Partition %s disconnected" % id)
        self.disconnectedPCAQueue.put(self.partitions[id])

    def checkIfRunning(self,clientObject):
        """check if client is Running"""
        if not (isinstance(clientObject,detectorDataObject) or isinstance(clientObject,partitionDataObject)):
            raise Exception("Expected detector or partition Object but got %s" % type(clientObject))
        id = clientObject.id
        address = clientObject.address
        fileName = self.checkIfRunningScript
        if isinstance(clientObject,detectorDataObject):
            path = self.pathToDetectorCodeFile
        else:
            path = self.pathToPCACodeFile
        ssh = None
        try:
            ssh = paramiko.SSHClient()
            ssh.load_system_host_keys()
            #login on pca computer
            ssh.connect(address,look_for_keys=False)
            #python script returns -1 if not running or the pid otherwise
            stdin, stdout, stderr = ssh.exec_command("pid=$(cd %s;./%s %s); echo $pid" % (path,fileName,id))
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
        if not (isinstance(clientObject,detectorDataObject) or isinstance(clientObject,partitionDataObject)):
            raise Exception("Expected detector or partition Object but got %s" % type(clientObject))
        id = clientObject.id
        address = clientObject.address
        if isinstance(clientObject,detectorDataObject):
            path = self.pathToDetectorCodeFile
            fileName = self.DetectorCodeFileName
        else:
            path = self.pathToPCACodeFile
            fileName = self.PCACodeFileName
        ssh = None
        try:
            pid = self.checkIfRunning(clientObject)
            if not pid:
                ssh = paramiko.SSHClient()
                ssh.load_system_host_keys()
                #login on pca computer
                ssh.connect(address,look_for_keys=False)
                stdin, stdout, stderr = ssh.exec_command("cd %s;./%s %s > /dev/null & echo $!" % (path,fileName,id))
                pid = stdout.readline()
                pid = pid.strip()
                ssh.close()
            else:
                self.log("Detector Client %s is already Running with PID %s" % (id,pid))
            self.clientProcesses[id] = pid
            return pid
        except Exception as e:
            if ssh:
                ssh.close()
            self.log("Exception executing ssh command: %s" % str(e))
            return False

    def stopClient(self,clientObject):
        """kills a PCA or Detector Client via SSH"""
        if not (isinstance(clientObject,detectorDataObject) or isinstance(clientObject,partitionDataObject)):
            raise Exception("Expected detector or partition Object but got %s" % type(clientObject))
        id = clientObject.id
        address = clientObject.address
        if id not in self.clientProcesses:
            self.log("tried to stop Client with an unknown ID %s " % id,True)
            return False
        ssh = None
        try:
            ssh = paramiko.SSHClient()
            ssh.load_system_host_keys()
            ssh.connect(address,look_for_keys=False)
            stdin, stdout, stderr = ssh.exec_command("kill %s" % (self.clientProcesses[id],))
            returnValue = stdout.channel.recv_exit_status()
            ssh.close()
            if returnValue == 0:
                return True
            else:
                self.log("error stopping Client for %s (probably wasn't running)" % id)
                return False
        except Exception as e:
            if ssh:
                ssh.close()
            self.log("Exception executing ssh command: %s" % str(e))
            return False



    def waitForRequests(self):
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

            def createPCA(arg):
                message = json.loads(arg)
                partition = partitionDataObject(json.loads(message["partition"]))
                ret = db.addPartition(partition)
                if ret == ECSCodes.ok:
                    #start PCA if Not Running
                    pid = self.checkIfRunning(partition)
                    if not pid:
                        pid = self.startClient(partition)
                    if not pid:
                        self.log("PCA Client for %s could not be startet" % partition.id,True)
                    #connect to pca
                    self.partitions[partition.id] = partition
                    self.socketSubscription.connect("tcp://%s:%i" % (partition.address,partition.portPublish))
                    #if not ECS_tools.getStateSnapshot(self.stateMap,partition.address,partition.portCurrentState,pcaid=partition.id,timeout=self.receive_timeout):
                    #let the reconnector thread handle the connection(in case of no connectivity the client request might take too long)
                    self.handleDisconnection(partition.id)
                    return ECSCodes.ok
                else:
                    return ret

            def deletePCA(arg):
                try:
                    message = json.loads(arg)
                    pcaId = message["partitionId"]
                    if "forceDelete" in message:
                        forceDelete = True
                    else:
                        forceDelete = False
                    partition = db.getPartition(pcaId)
                    if isinstance(partition,Exception) or partition == ECSCodes.idUnknown:
                        return partition
                    detectors = db.getDetectorsForPartition(pcaId)
                    if isinstance(detectors,Exception) or detectors == ECSCodes.idUnknown:
                        return detectors
                    #check if there are Detectors still assigned to the Partition
                    if len(detectors.asDictionary()) > 0:
                        raise Exception("Can not delete because there are still Detectors assigned to Partition")
                    ret = db.removePartition(pcaId)
                    if isinstance(ret,Exception):
                        return ret
                    #try to stop pca Client
                    self.stopClient(partition)
                    #remove from Maps
                    del self.partitions[partition.id]
                    del self.connectedPartitions[partition.id]
                    del self.disconnectedDetectors[partition.id]

                    return ECSCodes.ok
                except Exception as e:
                    self.log("Error Deleting Partition: %s" % str(e))
                    return e




            def remapDetector(arg):
                """moves a Detector between Partitions"""
                message = json.loads(arg)
                partitionId = message["partitionId"]
                detectorId = message["detectorId"]
                if "forceMove" in message:
                    forceMove = True
                else:
                    forceMove = False
                removed = False
                added = False
                dbChanged = False
                #is not assigned to any partition(unmapped)
                unused = False

                skipUnmap = False
                skipAdd = False

                oldPartition = db.getPartitionForDetector(detectorId)
                if oldPartition == ECSCodes.idUnknown:
                    unused = True
                else:
                    if oldPartition.id not in self.connectedPartitions:
                        if forceMove:
                            skipUnmap = True
                        else:
                            return ECSCodes.connectionProblemOldPartition
                #detector will be unmapped
                if partitionId == "unmapped":
                    newPartition = False
                else:
                    if partitionId not in self.connectedPartitions:
                        if forceMove:
                            skipAdd = True
                        else:
                            return ECSCodes.connectionProblemNewPartition
                    newPartition = self.partitions[partitionId]

                detector = db.getDetector(detectorId)

                print("remapping")
                try:
                    #change Database
                    if unused:
                        if db.mapDetectorToPCA(detectorId,newPartition.id) != ECSCodes.ok:
                            raise Exception("Error during changing Database")
                    elif not newPartition:
                        if db.unmapDetectorFromPCA(detectorId) != ECSCodes.ok:
                            raise Exception("Error during changing Database")
                    else:
                        if db.remapDetector(detectorId,newPartition.id,oldPartition.id) != ECSCodes.ok:
                            raise Exception("Error during changing Database")
                    print("db done")
                    dbChanged = True

                    if not unused:
                        if not skipUnmap:
                            #remove from Old Partition
                            try:
                                requestSocket = self.zmqContext.socket(zmq.REQ)
                                requestSocket.connect("tcp://%s:%s"  % (oldPartition.address,oldPartition.portCommand))
                                requestSocket.send_multipart([ECSCodes.removeDetector,detectorId.encode()])
                                ret = requestSocket.recv()
                                if ret != ECSCodes.ok:
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
                                requestSocket.send_multipart([ECSCodes.addDetector,detector.asJsonString().encode()])
                                ret = requestSocket.recv()
                                if ret != ECSCodes.ok:
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
                    print("added")
                    #inform DetectorController
                    requestSocket = self.zmqContext.socket(zmq.REQ)
                    requestSocket.connect("tcp://%s:%s"  % (detector.address,detector.portCommand))
                    if newPartition:
                        requestSocket.send_multipart([ECSCodes.detectorChangePartition,newPartition.asJsonString().encode()])
                    else:
                        requestSocket.send_multipart([ECSCodes.detectorChangePartition,self.unmappedDetectorControllerData.asJsonString().encode()])
                    try:
                        ret = requestSocket.recv()
                        if ret != ECSCodes.ok:
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
                    return ECSCodes.ok
                except Exception as e:
                    self.log("error during remapping:%s ;starting rollback for remapping Detector" % str(e),True)
                    #rollback
                    try:
                        if dbChanged:
                            if unused:
                                if db.unmapDetectorFromPCA(detectorId) != ECSCodes.ok:
                                    raise Exception("Error during changing Database")
                            elif not newPartition:
                                if db.mapDetectorToPCA(detectorId,oldPartition.id) != ECSCodes.ok:
                                    raise Exception("Error during changing Database")
                            else:
                                if db.remapDetector(detectorId,oldPartition.id,newPartition.id) != ECSCodes.ok:
                                    raise Exception("Error during changing Database")


                        if removed:
                            if not unused:
                                requestSocket = self.zmqContext.socket(zmq.REQ)
                                requestSocket.connect("tcp://%s:%s"  % (oldPartition.address,oldPartition.portCommand))
                                requestSocket.send_multipart([ECSCodes.addDetector,detector.asJsonString().encode()])
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
                                if ret != ECSCodes.ok:
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
                                    requestSocket.send_multipart([ECSCodes.removeDetector,detectorId.encode()])
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
                                if ret != ECSCodes.ok:
                                    self.log("%s returned error for removing Detector" % (newPartition.id),True)
                                    raise Exception("%s returned error for removing Detector" % (oldPartition.id))
                            else:
                                #remove from Unused Detectors
                                self.unmappedDetectorController.removeDetector(detectorId)
                        return e
                    except Exception as e:
                        self.log("Exception during roll back %s" %str(e),True)
                        return e

            def deleteDetector(arg):
                """deletes Detector enirely from System trys to shutdown the Detector;
                 with forceDelete User has the possibillity to delete from Databse without shutting down the detector;
                 only unmapped Detectors can be deleted"""
                message = json.loads(arg)
                detId = message['detectorId']
                if 'forceDelete' in message:
                    forceDelete = True
                else:
                    forceDelete = False

                detector = db.getDetector(detId)
                if isinstance(detector,Exception) or detector == ECSCodes.idUnknown:
                    return detector

                if not forceDelete:
                    #add to UnmappedDetectorController
                    if not self.unmappedDetectorController.isDetectorConnected(detector.id):
                        return ECSCodes.connectionProblemDetector
                    if not self.unmappedDetectorController.abortDetector(detector.id):
                        self.log("Detector %s could not be shutdown" % (detector.id))
                        return Exception("Detector %s could not be shutdown" % (detector.id))
                    if not self.unmappedDetectorController.isShutdown(detector.id):
                        if not self.unmappedDetectorController.shutdownDetector(detector.id):
                            self.log("Detector %s could not be shutdown" % (detector.id))
                            return Exception("Detector %s could not be shutdown" % (detector.id))
                self.unmappedDetectorController.removeDetector(detector.id)
                #delete from Database
                ret = db.removeDetector(detector.id)
                if ret != ECSCodes.ok:
                    return ret
                return ECSCodes.ok

            def createDetector(arg):
                dbChanged = False
                try:
                    dataObject = detectorDataObject(json.loads(arg))
                    ret = db.addDetector(dataObject)
                    if ret == ECSCodes.ok:
                        dbChanged = True
                        if self.unmappedDetectorController.checkIfTypeIsKnown(dataObject):
                            ret = self.unmappedDetectorController.addDetector(dataObject)
                            return ECSCodes.ok
                        else:
                            raise Exception("Detector Type is unknown")
                    return ret
                except Exception as e:
                    if dbChanged:
                        db.removeDetector(dataObject.id)
                    return e


            def getPartitionForDetector(arg):
                ret = db.getPartitionForDetector(arg)
                if ret == ECSCodes.idUnknown:
                    return self.unmappedDetectorControllerData
                else:
                    return ret

            def switcher(code,arg=None):
                #functions for codes
                dbFunctionDictionary = {
                    ECSCodes.pcaAsksForConfig: db.getPartition,
                    ECSCodes.detectorAsksForPCA: getPartitionForDetector,
                    ECSCodes.getDetectorForId: db.getDetector,
                    ECSCodes.pcaAsksForDetectorList: db.getDetectorsForPartition,
                    ECSCodes.getPartitionForId: db.getPartition,
                    ECSCodes.getAllPCAs: db.getAllPartitions,
                    ECSCodes.getUnmappedDetectors: db.getAllUnmappedDetetectos,
                    ECSCodes.createPartition: createPCA,
                    ECSCodes.deletePartition: deletePCA,
                    ECSCodes.createDetector: createDetector,
                    ECSCodes.deleteDetector: deleteDetector,
                    ECSCodes.detectorChangePartition: remapDetector,
                }
                #returns function for Code or None if the received code is unknown
                f = dbFunctionDictionary.get(code,None)
                try:
                    if not f:
                        self.log("received unknown command",True)
                        self.replySocket.send(ECSCodes.unknownCommand)
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

    def waitForUpdates(self):
        #watch subscription for further updates
        while True:
            try:
                m = self.socketSubscription.recv_multipart()
            except zmq.error.ContextTerminated:
                self.socketSubscription.close()
                break

            if len(m) != 3:
                self.log("received malformed update Message %s" % str(m))
                continue
            else:
                id, sequence, state = m
                id = id.decode()
                if state == ECSCodes.reset:
                    #delete PCA and Detectors associated with PCA From Map
                    db = DataBaseWrapper(self.log)
                    dets = db.getDetectorsForPartition(id).asDictionary()
                    arg = list(dets.keys())
                    arg.append(id)
                    self.stateMap.delMany(arg)
                    print("reset %s" % id)
                    continue
                if state == ECSCodes.removed:
                    del self.stateMap[id]
                    continue
                state = state.decode()
            sequence = ECS_tools.intFromBytes(sequence)
            print("received update",id, sequence, state)
            if id in self.stateMap:
                #only update if the current status sequence is smaller
                if self.stateMap[id][0] < sequence:
                    self.stateMap[id] = (sequence, state)
            else:
                self.stateMap[id] = (sequence, state)

    def checkPartition(self,partition):
        db = DataBaseWrapper(self.log)
        detectors = db.getDetectorsForPartition(partition.id)
        if isinstance(detectors,Exception):
            db.close()
            raise detectors
        if detectors == ECSCodes.idUnknown:
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
            requestSocket.send_multipart([ECSCodes.check,detectors.asJsonString().encode()])
            ret = requestSocket.recv()
        except zmq.Again:
            self.handleDisconnection(partition.id)
        except zmq.error.ContextTerminated:
            pass
        except Exception as e:
            self.log("error checking PCA %s: %s " % (partition.id,str(e)),True)
        finally:
            requestSocket.close()

    def checkDetector(self,detector):
        db = DataBaseWrapper(self.log)
        partition = db.getPartitionForDetector(detector.id)
        if isinstance(partition,Exception):
            db.close()
            raise detectors
        if partition == ECSCodes.idUnknown:
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
            requestSocket.send_multipart([ECSCodes.check,partition.asJsonString().encode()])
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
        db = DataBaseWrapper(self.log)
        unmappedDetectors = db.getAllUnmappedDetetectos()
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



    def log(self,logmessage,error=False):
        str=datetime.now().strftime("%Y-%m-%d %H:%M:%S")+":" + logmessage
        try:
            self.socketLogPublish.send(str.encode())
        except:
            self.socketLogPublish.close()
        if error:
            logging.critical(logmessage)
        else:
            logging.info(logmessage)

    def terminateECS(self):
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

if __name__ == "__main__":
    test = ECS()
    try:
        input()
        #test.terminateECS()
    except KeyboardInterrupt:
        test.terminateECS()
