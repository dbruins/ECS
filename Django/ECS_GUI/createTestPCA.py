#!/usr/bin/python3
import sys
sys.path.append("/home/daniel/Dokumente/Masterarbeit/ECS")
import sqlite3
from DataObjects import DataObjectCollection, DataObject, detectorDataObject, partitionDataObject, globalSystemDataObject, mappingDataObject
from DataBaseWrapper import DataBaseWrapper

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
