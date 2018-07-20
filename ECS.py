#!/usr/bin/python3
import threading
import copy

import sqlite3
from DataObjects import DataObjectCollection, detectorDataObject, partitionDataObject
class DataBaseWrapper:
    """Handler for the ECS Database"""
    connection = None

    def __init__(self):
        self.connection = sqlite3.connect("ECS_database.db")

    def getAllDetectors(self):
        """Get All Detectors in Detector Table"""
        c = self.connection.cursor()
        c.execute("SELECT * FROM Detector")
        res = c.fetchall()
        return DataObjectCollection(res,detectorDataObject)


    def getDetector(self,id):
        """get Detector with given id; returns None if it does not exist"""
        c = self.connection.cursor()
        val = (id,)
        res = c.execute("SELECT * FROM Detector WHERE id = ?", val).fetchone()
        return detectorDataObject(res)

    def getDetectorAddress(self,id):
        """get address of PCA with given address"""
        c = self.connection.cursor()
        val = (id,)
        try:
            res = c.execute("SELECT address FROM Detector WHERE id = ?", val).fetchone()
            return res[0]
        except Exception as e:
            print("error getting detector address %s: %s" % (str(id),str(e)))

    def getAllUnmappedDetetectos(self):
        """gets all Detectors which are currently unmmaped"""
        c = self.connection.cursor()
        try:
            res = c.execute("SELECT * FROM Detector Where Detector.id not in (select DetectorId From Mapping)").fetchall()
            return DataObjectCollection(res,detectorDataObject)
        except Exception as e:
            print("error getting unmapped detectors: %s" % str(e))

    def addDetector(self,dataObject):
        """add a Detector to Database"""
        c = self.connection.cursor()
        try:
            c.execute("INSERT INTO Detector VALUES (?,?,?,?)", dataObject.asArray())
            self.connection.commit()
            return True
        except Exception as e:
            print("error inserting values into Detector Table: %s" % str(e))
            self.connection.rollback()
            return False


    def removeDetector(self,id):
        """delete a Detector from Database"""
        c = self.connection.cursor()
        val = (id,)
        try:
            c.execute("DELETE FROM Detector WHERE id = ?", val)
            self.connection.commit()
        except Exception as e:
            print("error removing values from Detector Table: %s" % str(e))

    def getPartition(self,id):
        """Get Partition with given id from Database; returns None if it does not exist"""
        c = self.connection.cursor()
        val = (id,)
        try:
            res = c.execute("SELECT * FROM Partition WHERE id = ?", val).fetchone()
            return partitionDataObject(res)
        except Exception as e:
            print("error getting partition %s: %s" % (str(id),str(e)))

    def getPartitionAddress(self,id):
        """get address of PCA with given address"""
        c = self.connection.cursor()
        val = (id,)
        try:
            res = c.execute("SELECT address FROM Partition WHERE id = ?", val).fetchone()
            return res[0]
        except Exception as e:
            print("error getting partition address %s: %s" % (str(id),str(e)))

    def getAllPartitions(self):
        """Get All Detectors in Detector Table"""
        c = self.connection.cursor()

        try:
            c.execute("SELECT * FROM Partition")
            res = c.fetchall()
            return DataObjectCollection(res, partitionDataObject)
        except Exception as e:
            print("error getting all partitions: %s" % str(e))

    def getDetectorsForPartition(self,pcaId):
        """get all Mapped Detectors for a given PCA Id"""
        c = self.connection.cursor()
        val = (pcaId,)
        try:
            c.execute("SELECT * From Detector d join Mapping m on d.id = m.DetectorId Where PartitionId=?",val)
            res = c.fetchall()
            return DataObjectCollection(res, detectorDataObject)
        except Exception as e:
            print("error getting all detectors for Partition %s: %s" % str(pcaId), str(e))

    def addPartition(self,dataObject):
        """create new Partition"""
        c = self.connection.cursor()
        data = dataObject.asArray()
        try:
            c.execute("INSERT INTO Partition VALUES (?,?,?,?,?,?,?,?)", data)
            self.connection.commit()
            return True
        except Exception as e:
            print("error inserting values into Partition Table: %s" % str(e))
            self.connection.rollback()
            return False

    def removePartition(self,id):
        """delete a Partition with given id"""
        c = self.connection.cursor()
        val = (id,)
        try:
            c.execute("DELETE FROM Partition WHERE id = ?", val)
            #Free the Detectors
            c.execute("DELETE FROM Mapping WHERE PartitionId = ?", val)
            self.connection.commit()
        except Exception as e:
            print("error removing values from Detector Table: %s" % str(e))

    def mapDetectorToPCA(self,detId,pcaId):
        """map a Detector to a Partition"""
        c = self.connection.cursor()
        vals = (detId,pcaId)
        try:
            c.execute("INSERT INTO Mapping VALUES (?,?)", vals)
            self.connection.commit()
            return True
        except Exception as e:
            print("error mapping %s to %s: %s" % (str(detId),str(pcaId),str(e)))
            return False

    def unmapDetectorFromPCA(self,detId):
        """unmap a Detector from a Partition"""
        c = self.connection.cursor()
        val = (detId,)
        try:
            c.execute("DELETE FROM Mapping WHERE DetectorId = ?", val)
            self.connection.commit()
        except Exception as e:
            print("error unmapping %s: %s " % (str(detId),str(e)))

import zmq
import logging
import threading
import configparser
import ECSCodes
import struct
import json
from multiprocessing import Queue
import time
from ECS_tools import MapWrapper
#import DataObjects

class ECS:
    """The Experiment Control System"""
    def __init__(self):
        self.database = DataBaseWrapper()
        self.detectors = self.database.getAllDetectors()
        self.partitions = MapWrapper()
        partitions = self.database.getAllPartitions()
        for p in partitions:
            self.partitions[p.id] = p
        self.connectedPartitions = MapWrapper()
        self.stateMap = MapWrapper()

        self.commandSocketQueue = Queue()
        self.disconnectedPCAQueue = Queue()

        config = configparser.ConfigParser()
        config.read("init.cfg")
        conf = config["Default"]
        #todo configfile?
        self.receive_timeout = 2000
        self.pingIntervall = 2
        self.pingTimeout = 2000

        #subscribe to all PCAs
        self.ports = config["ZMQPorts"]
        self.zmqContext = zmq.Context()

        #socket for receiving requests from WebUI
        self.replySocket = self.zmqContext.socket(zmq.REP)
        #todo port out of config file
        self.replySocket.bind("tcp://*:%s" % "5000")

        #subscribe to all Partitions
        self.socketSubscription = self.zmqContext.socket(zmq.SUB)
        self.socketSubscription.setsockopt(zmq.SUBSCRIBE, b'')
        for p in self.partitions:
            address = p.address
            port = p.portPublish

            self.socketSubscription.connect("tcp://%s:%i" % (address,port))

        t = threading.Thread(name="updater", target=self.waitForUpdates)
        t.start()

        t = threading.Thread(name="requestHandler", target=self.waitForRequests)
        t.start()

        #get snapshots from PCAs
        for p in self.partitions:
            id = p.id
            address = p.address
            port = p.portCurrentState

            if not self.getStateSnapshot(id,address,port):
                self.handleDisconnection(id)

        self.reconnectorThread =  threading.Thread(name="reconnectorThread", target=self.reconnector)
        self.reconnectorThread.start()

        t = threading.Thread(name="commandHandler", target=self.commandSocketHandler)
        t.start()


    def commandSocketHandler(self):
        """send heartbeat/ping and commands on command socket"""
        nextPing = time.time() + self.pingIntervall
        while True:
            if not self.commandSocketQueue.empty():
                #sequential message processing might scale very badly if there a lot of pca especially if a pca has timeout
                id, command = self.commandSocketQueue.get()
                pca = self.partitions[id]
                socket = None
                socket = self.resetSocket(socket,pca.address,pca.portCommand,zmq.REQ)
                if command == ECSCodes.ping:
                    socket.setsockopt(zmq.RCVTIMEO, self.pingTimeout)
                else:
                    socket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)

                #try to send message
                socket.send(command)
                r = None
                try:
                    r = socket.recv()
                except zmq.Again:
                    self.handleDisconnection(id)
                if r != ECSCodes.ok:
                    print("received error for sending command: %s " % str(command))
                socket.close()
                if not r:
                    #try to resend later ?
                    #self.commandSocketQueue.put(m)
                    continue
                if command != ECSCodes.ping:
                    #we've just send a message we don't need a ping
                    nextPing = time.time() + self.pingIntervall
            if time.time() > nextPing:
                #it is time for a ping
                for id in self.connectedPartitions:
                    self.commandSocketQueue.put((id,ECSCodes.ping))
                    nextPing = time.time() + self.pingIntervall

    def reconnector(self):
        """Thread which trys to reconnect to PCAs"""
        while True:#not self.disconnectedPCAQueue.empty()
            pca = self.disconnectedPCAQueue.get()
            if not self.getStateSnapshot(pca.id,pca.address,pca.portCurrentState):
                self.disconnectedPCAQueue.put(pca)
            else:
                self.connectedPartitions[pca.id] = pca.id

    def handleDisconnection(self,id):
        del self.connectedPartitions[id]
        self.disconnectedPCAQueue.put(self.partitions[id])

    def waitForRequests(self):
        #SQLite objects created in a thread can only be used in that same thread. So we need a second connection -_-
        db = DataBaseWrapper()
        while True:
            m = self.replySocket.recv_multipart()
            if len(m) == 2:
                code, arg = m
                arg = arg.decode()
            elif len(m) == 1:
                code = m[0]
            else:
                print ("received malformed request message: %s", str(m))
                continue

            if code == ECSCodes.pcaAsksForConfig:
                ret = db.getPartition(arg).asJsonString()
                self.replySocket.send(ret.encode())

            if code == ECSCodes.detectorAsksForId:
                id = db.getDetectorIdForAddress(arg)
                self.replySocket.send(id.encode())

            if code == ECSCodes.pcaAsksForDetectorList:
                ret = db.getDetectorsForPartition(arg).asJsonString()
                self.replySocket.send(ret.encode())

            if code == ECSCodes.getAllPCAs:
                ret = db.getAllPartitions().asJsonString()
                self.replySocket.send(ret.encode())

            if code == ECSCodes.getUnmappedDetectors:
                ret = db.getAllUnmappedDetetectos().asJsonString()
                self.replySocket.send(ret.encode())

            if code == ECSCodes.createPartition:
                message = json.loads(arg)
                partition = partitionDataObject(json.loads(message["partition"]))
                detectors = message["detectors"]
                ret = db.addPartition(partition)
                if ret:
                    error = False
                    for detId in detectors:
                        ret = db.mapDetectorToPCA(detId,partition.id)
                        if not ret:
                            error = True
                    if error:
                        self.replySocket.send(ECSCodes.errorMapping)
                    else:
                        self.replySocket.send(ECSCodes.ok)
                    #connect to pca
                    self.partitions[partition.id] = partition
                    self.socketSubscription.connect("tcp://%s:%i" % (partition.address,partition.portPublish))
                    if not self.getStateSnapshot(partition.id,partition.address,partition.portCurrentState):
                        self.handleDisconnection(partition.id)
                else:
                    self.replySocket.send(ECSCodes.errorCreatingPartition)

            if code == ECSCodes.createDetector:
                obj = detectorDataObject(json.loads(arg))
                ret = db.addDetector(obj)
                if ret:
                    self.replySocket.send(ECSCodes.ok)
                else:
                    self.replySocket.send(ECSCodes.error)

            if code == ECSCodes.mapDetectorsToPCA:
                #detector id -> pca Id
                detectors = json.loads(arg)
                error = False
                for k,v in detectors.items():
                    ret = db.mapDetectorToPCA(k,v)
                    if not ret:
                        self.replySocket.send(ECSCodes.error)
                        error = True
                        break
                if not error:
                    self.replySocket.send(ECSCodes.ok)
                #todo add Detectors  to running system




    def receive_status(self,socket,pcaid):
        try:
            id, sequence, state = socket.recv_multipart()
        except zmq.Again:
            print ("timeout receiving status for %s" % pcaid)
            return None
        except Exception as e:
            print ("error receiving status for %s: %s" % (pcaid,str(e)))
            return None
        if id != b"":
            id = id.decode()
        else:
            id = None
        sequence = struct.unpack("!i",sequence)[0]
        if state != b"":
            state = state.decode()
        else:
            state = None
        return [id,sequence,state]

    def resetSocket(self,socket,address,port,type):
        """resets a socket with address and zmq Type; if socket is None a new socket will be created"""
        if socket != None:
            socket.close()
        socket = self.zmqContext.socket(type)
        socket.connect("tcp://%s:%s" % (address,port))
        if type == zmq.REQ:
            socket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        socket.setsockopt(zmq.LINGER,0)
        return socket

    def getStateSnapshot(self,pcaid,address,port):
        """get snapshot of State Table from a PCA this needs to happen to regard a PCA as connected"""
        socketGetCurrentStateTable = None
        socketGetCurrentStateTable = self.resetSocket(socketGetCurrentStateTable,address,port,zmq.DEALER)
        socketGetCurrentStateTable.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        socketGetCurrentStateTable.send(ECSCodes.hello)
        while True:
            ret = self.receive_status(socketGetCurrentStateTable,pcaid)
            if(ret == None):
                socketGetCurrentStateTable.close()
                return False

            id, sequence, state = ret
            print (id,sequence,state)
            if id != None:
                self.stateMap[id] = (sequence, state)
            #id should be None in final message
            else:
                #todo this is kind of stupid
                self.connectedPartitions[pcaid] = pcaid
                socketGetCurrentStateTable.close()
                return True

    def waitForUpdates(self):
        #watch subscription for further updates
        while True:
            m = self.socketSubscription.recv_multipart()
            if len(m) != 3:
                print (m)
            else:
                id = m[0].decode()
                sequence = m[1]
                state = m[2].decode()
            sequence = struct.unpack("!i",sequence)[0]
            print("received update",id, sequence, state)
            #todo do something with the update
            #stateMap[id] = (sequence, state)



if __name__ == "__main__":
    test = ECS()
    """
    test.database.addDetector("1", "localhost", "DetectorA",5558)
    test.database.addDetector("5677", "localhost", "DetectorA", 5558)
    test.database.addDetector("1", "localhost", "DetectorA", 55511)
    test.database.addDetector("2", "localhost", "DetectorA",5559)
    #test.database.addPartition("pca1", "localhost")
    #test.database.addPartition("pca2", "localhost")
    test.database.mapDetectorToPCA(1,"pca1")
    test.database.mapDetectorToPCA(1,"pca2")
    test.database.mapDetectorToPCA(2,"pca1")
    test.database.addDetector("2", "localhost", "DetectorA",5559)
    """
    input()
