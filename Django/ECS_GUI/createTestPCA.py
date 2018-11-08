#!/usr/bin/python3
import sys
sys.path.append("/home/daniel/Dokumente/Masterarbeit/ECS")
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
            c.execute("DELETE FROM Mapping WHERE DetectorId = ?", (detId,))
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

def logDummy(log,vool):
    pass
db = DataBaseWrapper(logDummy)
if "clean" in sys.argv:
    detectors = db.getDetectorsForPartition("test")
    for d in detectors:
        db.removeDetector(d.id)
        print(d.id +" removed")
    db.removePartition("test")
    db.close()
    exit(0)

detectorCount = int(sys.argv[1])

startPort=80000
startId = 50

pca=partitionDataObject(["test","localhost",79000,79001,79002,79003,79004,79005])
db.addPartition(pca)

for i in range(startId, startId+detectorCount):
    det = detectorDataObject([str(i),"localhost","DetectorA",startPort,startPort+1])
    startPort=startPort+2
    db.addDetector(det)
    db.mapDetectorToPCA(det.id,pca.id)
    print(det.id +" added")
db.close()
