import sqlite3
from DataObjects import DataObjectCollection, DataObject, detectorDataObject, partitionDataObject, globalSystemDataObject, mappingDataObject, configObject
from ECSCodes import ECSCodes
codes = ECSCodes()
import json
try:
    from django.core.exceptions import ImproperlyConfigured
    #when executed from Django
    from django.conf import settings
    dataBaseFile = settings.ECS_DATABASE
except ModuleNotFoundError:
    dataBaseFile = "ECS_database.db"
except ImproperlyConfigured:
    dataBaseFile = "ECS_database.db"


class DataBaseWrapper:
    """Handler for the ECS Database"""
    connection = None

    def __init__(self,logfunction):
        self.log = logfunction
        self.dataBaseFile = dataBaseFile

    def handleError(self, exception, errorMessage):
        #full exception to log
        self.log(errorMessage+": %s" % str(errorMessage),True)
        #error message shown to user
        return Exception(errorMessage)

    def getAllDetectors(self):
        """Get All Detectors in Detector Table; returns empty DataObjectCollection if there are now Detectors"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        try:
            c.execute("SELECT * FROM Detector")
            res = c.fetchall()
            return DataObjectCollection(res,detectorDataObject)
        except Exception as e:
            return self.handleError(e,"error getting detectors")
        finally:
            connection.close()

    def getDetector(self,id):
        """get Detector with given id; returns ErrorCode if it does not exist"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        val = (id,)
        try:
            res = c.execute("SELECT * FROM Detector WHERE id = ?", val).fetchone()
            if not res:
                return codes.idUnknown
            return detectorDataObject(res)
        except Exception as e:
            return self.handleError(e,"error getting detector")
        finally:
            connection.close()

    def getAllUnmappedDetectors(self):
        """gets all Detectors which are currently unmmaped"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        try:
            res = c.execute("SELECT * FROM Detector Where Detector.id not in (select DetectorId From Mapping)").fetchall()
            return DataObjectCollection(res,detectorDataObject)
        except Exception as e:
            return self.handleError(e,"error getting unmapped detectors")
        finally:
            connection.close()

    def addDetector(self,dataObject):
        """add a Detector to Database;accepts json String or DataObject"""
        if not isinstance(dataObject,detectorDataObject):
            dataObject = detectorDataObject(json.loads(dataObject))
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        try:
            c.execute("INSERT INTO Detector VALUES (?,?,?,?)", dataObject.asArray())
            connection.commit()
            return codes.ok
        except Exception as e:
            connection.rollback()
            return self.handleError(e,"error inserting values into Detector Table")
        finally:
            connection.close()

    def removeDetector(self,id):
        """delete a Detector from Database"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        val = (id,)
        try:
            c.execute("DELETE FROM Detector WHERE id = ?", val)
            connection.commit()
            return codes.ok
        except Exception as e:
            return self.handleError(e,"error removing values from Detector Table")
        finally:
            connection.close()
    def getPartition(self,id):
        """Get Partition with given id from Database; returns None if it does not exist"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        val = (id,)
        try:
            res = c.execute("SELECT * FROM Partition WHERE id = ?", val).fetchone()
            if not res:
                return codes.idUnknown
            return partitionDataObject(res)
        except Exception as e:
            return self.handleError(e,"error getting partition")
        finally:
            connection.close()

    def getPartitionForDetector(self,id):
        """gets the Partition of a Detector; returns DataObject or ErrorCode"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        val = (id,)
        try:
            res = c.execute("SELECT * FROM Partition WHERE Partition.id IN (SELECT PartitionId FROM (Mapping JOIN Partition ON Mapping.PartitionId = Partition.id) WHERE DetectorId = ?)", val).fetchone()
            if not res:
                return codes.idUnknown
            return partitionDataObject(res)
        except Exception as e:
            return self.handleError(e,"error getting partition for Detector")
        finally:
            connection.close()

    def getAllPartitions(self):
        """Get All Detectors in Detector Table"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        try:
            c.execute("SELECT * FROM Partition")
            res = c.fetchall()
            return DataObjectCollection(res, partitionDataObject)
        except Exception as e:
            return self.handleError(e,"error getting partitions")
        finally:
            connection.close()

    def getDetectorsForPartition(self,pcaId):
        """get all Mapped Detectors for a given PCA Id"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        val = (pcaId,)
        try:
            c.execute("SELECT * From Detector WHERE Detector.id in (SELECT d.id FROM Detector d JOIN Mapping m ON d.id = m.DetectorId WHERE PartitionId=?)",val)
            res = c.fetchall()
            return DataObjectCollection(res, detectorDataObject)
        except Exception as e:
            return self.handleError(e,"error getting detectors for Partition")
        finally:
            connection.close()

    def addPartition(self,dataObject):
        """create new Partition"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        data = dataObject.asArray()
        try:
            c.execute("INSERT INTO Partition VALUES (?,?,?,?,?,?,?)", data)
            connection.commit()
            return codes.ok
        except Exception as e:
            connection.rollback()
            return self.handleError(e,"error inserting values into Partition Table")
        finally:
            connection.close()

    def removePartition(self,id):
        """delete a Partition with given id"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        val = (id,)
        try:
            c.execute("DELETE FROM Partition WHERE id = ?", val)
            #Free the Detectors
            c.execute("DELETE FROM Mapping WHERE PartitionId = ?", val)
            connection.commit()
            return codes.ok
        except Exception as e:
            connection.rollback()
            return self.handleError(e,"error removing values from Partition Table")
        finally:
            connection.close()

    def getDetectorMapping(self):
        """get entire PCA Detector Mapping Table"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        try:
            c.execute("SELECT * From Mapping")
            res = c.fetchall()
            return DataObjectCollection(res, mappingDataObject)
        except Exception as e:
            return self.handleError(e,"error getting Mapping Table")
        finally:
            connection.close()

    def mapDetectorToPCA(self,detId,pcaId):
        """map a Detector to a Partition"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        vals = (detId,pcaId)
        try:
            c.execute("INSERT INTO Mapping VALUES (?,?)", vals)
            connection.commit()
            return codes.ok
        except Exception as e:
            connection.rollback()
            return self.handleError(e,"error mapping %s to %s" % (str(detId),str(pcaId),))
        finally:
            connection.close()

    def remapDetector(self,detId,newPcaId,oldPcaID):
        """assign Detector to a different Partition"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        vals = (detId,newPcaId)
        try:
            c.execute("DELETE FROM Mapping WHERE DetectorId = ?", (detId,))
            c.execute("INSERT INTO Mapping VALUES (?,?)", vals)
            connection.commit()
            return codes.ok
        except Exception as e:
            connection.rollback()
            return self.handleError(e,"error remapping %s from %s to %s" % (str(detId),str(oldPcaID),str(newPcaId),))
        finally:
            connection.close()

    def unmapDetectorFromPCA(self,detId):
        """unmap a Detector from a Partition"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        val = (detId,)
        try:
            c.execute("DELETE FROM Mapping WHERE DetectorId = ?", val)
            connection.commit()
            return codes.ok
        except Exception as e:
            connection.rollback()
            return self.handleError(e,"error unmapping %s" % str(detId))
        finally:
            connection.close()

    def usedPortsForAddress(self,address):
        """get all used Ports for an Ip-Address returns List of Ports or ErrorCode """
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
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
            return self.handleError(e,"error getting Ports for Address %s" % address)
        finally:
            connection.close()

    def getGlobalSystem(self,id):
        """get global System for Id"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        val = (id,)
        try:
            res = c.execute("SELECT * From GlobalSystems Where id=?",val).fetchone()
            if not res:
                return codes.idUnknown
            return globalSystemDataObject(res)
        except Exception as e:
            return self.handleError(e,"error getting Global System")
        finally:
            connection.close()

    def getAllGlobalSystems(self):
        """get all global systems"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        try:
            c.execute("SELECT * FROM GlobalSystems")
            res = c.fetchall()
            return DataObjectCollection(res,globalSystemDataObject)
        except Exception as e:
            return self.handleError(e,"error getting Global Systems")
        finally:
            connection.close()

    def getPcaCompatibleTags(self,pcaId):
        """get all tags for a pca which are compatible with the current Detector Assignment"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        val = (pcaId,pcaId)
        try:
            res =c.execute("""
            SELECT distinct tagname FROM ConfigurationTag as x WHERE
              NOT EXISTS
               (SELECT detectorId FROM
            					  (SELECT detectorId FROM Partition join Mapping on Partition.id = Mapping.PartitionId WHERE partitionId = ?
            						union
            					  SELECT id FROM GlobalSystems)
                WHERE detectorId NOT in (SELECT systemId FROM ConfigurationTag Join Configurations on ConfigurationTag.configId = Configurations.configId WHERE tagname=x.tagname))
              and NOT EXISTS
                (SELECT systemId FROM
                      ConfigurationTag Join Configurations on ConfigurationTag.configId = Configurations.configId
                 WHERE tagname=x.tagname and systemId NOT in
                (SELECT detectorId FROM ( SELECT detectorId FROM Partition join Mapping on Partition.id = Mapping.PartitionId WHERE partitionId = ?
                 union
            	   SELECT id FROM GlobalSystems)))
            """.replace("\n",""),val).fetchall()
            if not res:
                return None
            #result elements have only one entry so no need for nestet arrays
            return set(map(lambda x:x[0],res))
        except Exception as e:
            return self.handleError(e,"error getting pca %s tags" % pcaId)
        finally:
            connection.close()

    def getAllConfigTags(self):
        """gets all config tags"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        try:
            res = c.execute("SELECT distinct TagName FROM ConfigurationTag").fetchall()
            if not res or len(res) == 0:
                return codes.idUnknown
            return list(map(lambda x:x[0],res))
        except Exception as e:
            return self.handleError(e,"error getting tags")
        finally:
            connection.close()

    def getConfigsForTag(self,tag):
        """get the subsystem configurations for a tag"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        val = (tag,)
        try:
            res = c.execute("SELECT Configurations.configId,Configurations.systemId,parameters FROM Configurations join ConfigurationTag on Configurations.configId = ConfigurationTag.configId where tagname = ?",val).fetchall()
            if not res or len(res) == 0:
                return codes.idUnknown
            #load json from parameters string
            res = list(map(lambda x:x[:2]+(json.loads(x[2]),),res))
            return DataObjectCollection(res, configObject)
        except Exception as e:
            return self.handleError(e,"error getting configurations for tag %s" % tag)
        finally:
            connection.close()

    def getConfigsForSystem(self,systemId):
        """get all possible Configurations for a System"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        val = (systemId,)
        try:
            res =c.execute("SELECT * FROM Configurations WHERE systemId=?",val).fetchall()
            if not res:
                return codes.idUnknown
            #load json from parameters string
            res = list(map(lambda x:x[:2]+(json.loads(x[2]),),res))
            return DataObjectCollection(res, configObject)
        except Exception as e:
            return self.handleError(e,"error getting configurations for %s" % systemId)
        finally:
            connection.close()

    def getConfigsForManySystems(self,systemList):
        """get all possible Configurations for a List of Systems"""
        connection = sqlite3.connect("ECS_database.db")
        c = connection.cursor()
        try:
            res =c.execute("SELECT * from Configurations where systemId in (%s)" % " ,".join(list(map(lambda x:"?",systemList))),systemList).fetchall()
            if not res:
                return codes.idUnknown
            #load json from parameters string
            res = list(map(lambda x:x[:2]+(json.loads(x[2]),),res))
            return DataObjectCollection(res, configObject)
        except Exception as e:
            return self.handleError(e,"error getting Configurations")
        finally:
            connection.close()

    def getConfigsForPCA(self,pcaId):
        """get all possible Configs for Partition Systems"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        val = (pcaId,)
        try:
            res =c.execute("""SELECT configid,detectorid,parameters FROM ((Partition Join Mapping on Partition.id = Mapping.PartitionId) join Detector on detectorId=Detector.id) left join Configurations on detectorid = Configurations.systemId  Where Partitionid=?
                              union
                              select configid,id,parameters FROM GlobalSystems left join Configurations on GlobalSystems.id = Configurations.systemId
                           """.replace("\n",""),val).fetchall()
            if not res:
                return codes.idUnknown
            #parameters(res[2]) are stored as a json string in the database; there could be no config for a system
            res = list(map(lambda x:x[:2]+(json.loads(x[2]),) if x[2] != None else x ,res))
            return DataObjectCollection(res, configObject)
        except Exception as e:
            return self.handleError(e,"error getting Configurations for %s" % pcaId)
        finally:
            connection.close()

    def getCustomConfig(self,configList):
        """get Configs for a list of ConfigIds"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        try:
            res =c.execute("SELECT * from Configurations where Configurations.configId in (%s)" % " ,".join(list(map(lambda x:"?",configList))),configList).fetchall()
            #parameters(res[2]) are stored as a json string in the database
            res = list(map(lambda x:x[:2]+(json.loads(x[2]),),res))
            return DataObjectCollection(res, configObject)
        except Exception as e:
            return self.handleError(e,"error getting All Costum Configurations")
        finally:
            connection.close()

    def saveConfigTag(self,tagName,configList):
        """saves the configList in the database under the name tagName"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        try:
            self.deleteConfigTag(tagName)
            res =c.execute("INSERT INTO ConfigurationTag VALUES %s" % " ,".join(list(map(lambda x:"('%s',?)" % tagName,configList))),configList)
            connection.commit()
            return codes.ok
        except Exception as e:
            connection.rollback()
            return self.handleError(e,"error saving config tag")
        finally:
            connection.close()

    def deleteConfigTag(self,tagName):
        """delete a configuration tag in the database"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        val = (tagName,)
        try:
            res =c.execute("DELETE FROM ConfigurationTag WHERE ConfigurationTag.TagName=?",val)
            connection.commit()
            return codes.ok
        except Exception as e:
            connection.rollback()
            return self.handleError(e,"error deleting configuration tag")
        finally:
            connection.close()

    def saveConfig(self,configId,systemId,params):
        """save or alter a configuration in the database"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        vals = (configId,systemId,params)
        try:
            res =c.execute("INSERT OR REPLACE INTO Configurations VALUES (?,?,?)",vals)
            connection.commit()
            return codes.ok
        except Exception as e:
            connection.rollback()
            return self.handleError(e,"error saving configuration")
        finally:
            connection.close()

    def deleteConfig(self,configId):
        """delete a configuration in the database"""
        connection = sqlite3.connect(self.dataBaseFile)
        c = connection.cursor()
        val = (configId,)
        try:
            res =c.execute("DELETE FROM Configurations WHERE configId=?",val)
            connection.commit()
            return codes.ok
        except Exception as e:
            connection.rollback()
            return self.handleError(e,"error deleting configuration")
        finally:
            connection.close()
