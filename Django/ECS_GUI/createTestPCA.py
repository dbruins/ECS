#!/usr/bin/python3
import sys
sys.path.append("/home/bruins/ECS/")
import sqlite3
from DataObjects import DataObjectCollection, DataObject, detectorDataObject, partitionDataObject, globalSystemDataObject, mappingDataObject
from DataBaseWrapper import DataBaseWrapper

def logDummy(a,b):
    print(a)
db = DataBaseWrapper(logDummy)
if "clean" in sys.argv:
    detectors = db.getDetectorsForPartition("test")
    for d in detectors:
        db.removeDetector(d.id)
        print(d.id +" removed")
        db.deleteConfig("config_%s" % d.id)
    db.removePartition("test")
    db.deleteConfigTag("TestTag")
    exit(0)

detectorCount = int(sys.argv[1])
#nodeList = ["pn02","pn04","pn06","pn07","pn08"]
nodeList = ["pn30","pn31","pn32","pn33","pn34"]   
nodeCount = len(nodeList)

detectorsPerNode = int(detectorCount / nodeCount)
rest = detectorCount % nodeCount
startNode = 4

startPort=80000
startId = 50

pca=partitionDataObject(["test","pn35",79000,79001,79002,79003,79004,79005])
db.addPartition(pca)
configList=["testFLES","testDCS","testQA","testTFC"]
for i in nodeList:
    extra=0
    if rest > 0:
      extra=1
      rest-=1    
    for j in range(startId, startId+detectorsPerNode+extra):
        det = detectorDataObject([str(j),i,"DetectorA",startPort,startPort+1])
        db.saveConfig("config_%s" % det.id,det.id,'{"pam1":"v1","pam2":"v2"}')
        configList.append("config_%s" % det.id)
        startPort=startPort+2
        db.addDetector(det)
        db.mapDetectorToPCA(det.id,pca.id)
        print(det.id +" added at "+i)
    startId = startId + detectorsPerNode
db.saveConfigTag("TestTag",configList)
