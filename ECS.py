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
        self.detectors = self.database.getAllDetectors()
        self.partitions = ECS_tools.MapWrapper()
        partitions = self.database.getAllPartitions()
        for p in partitions:
            self.partitions[p.id] = p
        self.connectedPartitions = ECS_tools.MapWrapper()
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

        #subscribe to all PCAs
        self.zmqContext = zmq.Context()

        #socket for receiving requests from WebUI
        self.replySocket = self.zmqContext.socket(zmq.REP)
        #todo port out of config file
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
        self.socketSubscription = self.zmqContext.socket(zmq.SUB)
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
            if ret:
                self.clientProcesses[p.id] = ret


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
                    socket.setsockopt(zmq.RCVTIMEO, self.pingTimeout)
                    socket.setsockopt(zmq.LINGER,0)
                    socket.send(ECSCodes.ping)
                    r = socket.recv()
                except zmq.Again:
                    self.handleDisconnection(id)
                except zmq.error.ContextTerminated:
                    socket.close()
                    break
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
                detectors = message["detectors"]
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

            def mapDetectorsToPCA(arg):
                """map one or more Detectors to PCA"""
                detectors = json.loads(arg)
                for detId,pcaId in detectors.items():
                    detector = db.getDetector(detId)
                    partition = self.partitions[pcaId]
                    dbChanged = False
                    added = False
                    try:
                        ret = db.mapDetectorToPCA(detId,pcaId)
                        if ret == ECSCodes.error:
                            raise Exception("Error during Databse operation")
                        dbChanged = True
                        print("db changed")
                        #add to Partition
                        requestSocket = self.zmqContext.socket(zmq.REQ)
                        requestSocket.connect("tcp://%s:%s"  % (partition.address,partition.portCommand))
                        #requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
                        requestSocket.send_multipart([ECSCodes.addDetector,detector.asJsonString().encode()])
                        try:
                            ret = requestSocket.recv()
                            requestSocket.close()
                        except zmq.Again:
                            self.log("timeout adding Detector to %s" % (partition.id),True)
                            requestSocket.close()
                            raise Exception("Error adding Detector")
                        except Exception as e:
                            self.log("error adding Detector to %s: %s " % (partition.id,str(e)),True)
                            requestSocket.close()
                            raise Exception("Error adding Detector")
                        added = True
                        print("added")
                        #inform DetectorController
                        requestSocket = self.zmqContext.socket(zmq.REQ)
                        requestSocket.connect("tcp://%s:%s"  % (detector.address,detector.portCommand))
                        requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
                        requestSocket.send_multipart([ECSCodes.detectorChangePartition,partition.asJsonString().encode()])
                        try:
                            ret = requestSocket.recv()
                            requestSocket.close()
                        except zmq.Again:
                            self.log("timeout changing Detector %s PCA" % (detector.id),True)
                            requestSocket.close()
                            raise Exception("Error informing Detector")
                        except Exception as e:
                            self.log("error changing Detector %s PCA: %s " % (detector.id),str(e),True)
                            requestSocket.close()
                            raise Exception("Error informing Detector")
                        if ret != ECSCodes.ok:
                            self.log("%s returned error for changing PCA" % (detector.id),True)
                            requestSocket.close()
                            raise Exception("Error informing Detector")
                        requestSocket.close()
                    except Exception as e:
                        self.log("Exception during mapping Detector %s: %s; starting rollback" % (detId,str(e)))
                        try:
                            if dbChanged:
                                ret = db.unmapDetectorFromPCA(detId)
                                if ret == ECSCodes.error:
                                    raise Exception("Error during Databse operation")
                            if added:
                                requestSocket = self.zmqContext.socket(zmq.REQ)
                                requestSocket.connect("tcp://%s:%s"  % (partition.address,partition.portCommand))
                                requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
                                requestSocket.send_multipart([ECSCodes.removeDetector,detector.asJsonString().encode()])
                                try:
                                    ret = requestSocket.recv()
                                    requestSocket.close()
                                except zmq.Again:
                                    self.log("timeout removing Detector from %s" % (partition.id),True)
                                    requestSocket.close()
                                    raise Exception("Error removing Detector")
                                except Exception as e:
                                    self.log("error removing Detector from %s: %s " % (partition.id,str(e)),True)
                                    requestSocket.close()
                                    raise Exception("Error removing Detector")
                            return ECSCodes.error
                        except Exception as e:
                            self.log("Exception during rollback %s" % str(e))
                            return ECSCodes.error
                return ECSCodes.ok

            def remapDetector(arg):
                """moves a Detector between Partitions"""
                message = json.loads(arg)
                partitionId = message["partitionId"]
                detectorId = message["detectorId"]
                removed = False
                added = False
                dbChanged = False
                unused = False
                oldPartition = db.getPartitionForDetector(detectorId)
                if oldPartition == ECSCodes.idUnknown:
                    unused = True
                if partitionId == "unmapped":
                    newPartition = False
                else:
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
                        #remove from Old Partition
                        requestSocket = self.zmqContext.socket(zmq.REQ)
                        requestSocket.connect("tcp://%s:%s"  % (oldPartition.address,oldPartition.portCommand))
                        requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
                        requestSocket.send_multipart([ECSCodes.removeDetector,detectorId.encode()])
                        try:
                            ret = requestSocket.recv()
                        except zmq.Again:
                            self.log("timeout removing Detector from %s" % (oldPartition.id),True)
                            requestSocket.close()
                            raise Exception("Error during removing")
                        except Exception as e:
                            self.log("error removing Detector from %s: %s " % (oldPartition.id,str(e)),True)
                            requestSocket.close()
                            raise Exception("Error during removing")
                        finally:
                            requestSocket.close()
                        if ret != ECSCodes.ok:
                            self.log("%s returned error for removing Detector" % (oldPartition.id),True)
                            requestSocket.close()
                            raise Exception("Error during removing")
                        requestSocket.close()
                    else:
                        #remove from Unused Detectors
                        self.unmappedDetectorController.removeDetector(detectorId)
                    removed = True

                    if newPartition:
                        #add to new Partition
                        requestSocket = self.zmqContext.socket(zmq.REQ)
                        requestSocket.connect("tcp://%s:%s"  % (newPartition.address,newPartition.portCommand))
                        requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
                        requestSocket.send_multipart([ECSCodes.addDetector,detector.asJsonString().encode()])
                        try:
                            ret = requestSocket.recv()
                            requestSocket.close()
                        except zmq.Again:
                            self.log("timeout adding Detector to %s" % (newPartition.id),True)
                            requestSocket.close()
                            raise Exception("Error during adding")
                        except Exception as e:
                            self.log("error adding Detector to %s: %s " % (newPartition.id,str(e)),True)
                            requestSocket.close()
                            raise Exception("Error during adding")
                        if ret != ECSCodes.ok:
                            self.log("%s returned error for adding Detector" % (newPartition.id),True)
                            requestSocket.close()
                            raise Exception("Error during adding")
                        requestSocket.close()
                    else:
                        #add to unused detectors
                        self.unmappedDetectorController.addDetector(detector)
                    added = True

                    #inform DetectorController
                    requestSocket = self.zmqContext.socket(zmq.REQ)
                    requestSocket.connect("tcp://%s:%s"  % (detector.address,detector.portCommand))
                    requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
                    requestSocket.send_multipart([ECSCodes.detectorChangePartition,newPartition.asJsonString().encode()])
                    try:
                        ret = requestSocket.recv()
                        requestSocket.close()
                    except zmq.Again:
                        self.log("timeout changing Detector %s PCA" % (detector.id),True)
                        requestSocket.close()
                        raise Exception("Error during informing Detector")
                    except Exception as e:
                        self.log("error changing Detector %s PCA: %s " % (detector.id),str(e),True)
                        requestSocket.close()
                        raise Exception("Error during informing Detector")
                    if ret != ECSCodes.ok:
                        self.log("%s returned error for changing PCA" % (detector.id),True)
                        requestSocket.close()
                        raise Exception("Error during informing Detector")
                    requestSocket.close()
                    return ECSCodes.ok
                except Exception as e:
                    self.log("error during remapping:%s \n starting rollback for remapping Detectors" % str(e),True)
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
                                requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
                                requestSocket.send_multipart([ECSCodes.addDetector,detector.asJsonString().encode()])
                                try:
                                    ret = requestSocket.recv()
                                    requestSocket.close()
                                except zmq.Again:
                                    self.log("timeout adding Detector to %s" % (oldPartition.id),True)
                                    requestSocket.close()
                                    return e
                                except Exception:
                                    self.log("error adding Detector to %s: %s " % (oldPartition.id,str(e)),True)
                                    requestSocket.close()
                                    return e
                                if ret != ECSCodes.ok:
                                    self.log("%s returned error for adding Detector" % (oldPartition.id),True)
                                    requestSocket.close()
                                    return e
                                requestSocket.close()
                            else:
                                #add to unused detectors
                                self.unmappedDetectorController.addDetector(detector)
                        if added:
                            if newPartition:
                                requestSocket = self.zmqContext.socket(zmq.REQ)
                                requestSocket.connect("tcp://%s:%s"  % (newPartition.address,newPartition.portCommand))
                                requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
                                requestSocket.send_multipart([ECSCodes.removeDetector,detectorId.encode()])
                                try:
                                    ret = requestSocket.recv()
                                except zmq.Again:
                                    self.log("timeout removing Detector from %s" % (newPartition.id),True)
                                    requestSocket.close()
                                    return e
                                except Exception:
                                    self.log("error removing Detector from %s: %s " % (newPartition.id,str(e)),True)
                                    requestSocket.close()
                                    return e
                                finally:
                                    requestSocket.close()
                                if ret != ECSCodes.ok:
                                    self.log("%s returned error for removing Detector" % (newPartition.id),True)
                                    requestSocket.close()
                                    return e
                                requestSocket.close()
                            else:
                                #remove from Unused Detectors
                                self.unmappedDetectorController.removeDetector(detectorId)
                        return e
                    except Exception as e:
                        self.log("Exception during roll back %s" %str(e),True)
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
                    ECSCodes.createDetector: db.addDetector,
                    ECSCodes.mapDetectorsToPCA: mapDetectorsToPCA,
                    ECSCodes.detectorChangePartition: remapDetector,
                }
                #returns function for Code or None if the received code is unknown
                f = dbFunctionDictionary.get(code,None)
                try:
                    if not f:
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
                print (m)
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
        self.zmqContext.term()

if __name__ == "__main__":
    test = ECS()
    try:
        input()
        #test.terminateECS()
    except KeyboardInterrupt:
        test.terminateECS()
