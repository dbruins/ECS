#!/usr/bin/python3
from DataObjects import DataObjectCollection, globalSystemDataObject, mappingDataObject, partitionDataObject
from states import DCSStates,DCSTransitions, FLESStates, FLESTransitions, QAStates, QATransitions, TFCStates, TFCTransitions
import subprocess
import zc.lockfile
import time
from ECSCodes import ECSCodes
codes = ECSCodes()
import configparser
from Statemachine import Statemachine
import json
import zmq
import threading
import _thread
import sys
import re
import time

class GlobalSystemClient:
    def __init__(self,type):
        #create lock
        try:
            self.lock = zc.lockfile.LockFile('/tmp/lock'+type, content_template='{pid}')
        except zc.lockfile.LockError:
            print("other Process is already Running "+type)
            exit(1)

        #get pca data from ECS
        config = configparser.ConfigParser()
        config.read("init.cfg")
        self.conf = config["Default"]
        self.receive_timeout = int(self.conf['receive_timeout'])

        self.context = zmq.Context()
        self.scriptProcess = None
        self.abort = False

        mapping = None
        #ask ECS For required Data
        while mapping == None:
            try:
                requestSocket = self.context.socket(zmq.REQ)
                requestSocket.connect("tcp://%s:%s" % (self.conf['ECSAddress'],self.conf['ECSRequestPort']))
                requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
                requestSocket.setsockopt(zmq.LINGER,0)

                requestSocket.send_multipart([codes.getAllPCAs])
                partitions = requestSocket.recv()
                partitions = DataObjectCollection(json.loads(partitions.decode()),partitionDataObject)

                requestSocket.send_multipart([codes.GlobalSystemAsksForInfo,type.encode()])
                globalSystemInfo = requestSocket.recv()
                globalSystemInfo = globalSystemDataObject(json.loads(globalSystemInfo.decode()))

                requestSocket.send_multipart([codes.getDetectorMapping])
                mapping = requestSocket.recv()
                mapping = DataObjectCollection(json.loads(mapping.decode()),mappingDataObject)
            except zmq.Again:
                print("timeout getting %s Data" % type)
                continue
            except zmq.error.ContextTerminated:
                pass
            finally:
                requestSocket.close()

        self.detectorMapping = {}
        for m in mapping:
            self.detectorMapping[m.detectorId] = m.partitionId

        self.MyId = globalSystemInfo.id

        configDet = configparser.ConfigParser()
        configDet.read("detector.cfg")
        configDet = configDet[type]
        self.StateMachineFile = configDet["stateFile"]
        self.StateMachineForPca = {}
        self.PCAs = {}
        self.isPCAinTransition = {}
        self.pcaConfigTag = {}
        for p in partitions:
            self.PCAs[p.id] = p
            self.StateMachineForPca[p.id] = Statemachine(self.StateMachineFile,"Unconfigured")
            self.isPCAinTransition[p.id] = False

        self.commandSocket = self.context.socket(zmq.REP)
        self.commandSocket.bind("tcp://*:%i" % globalSystemInfo.portCommand)

        #send update to All PCAs
        self.sendUpdateToAll()

        t = threading.Thread(name="waitForCommands", target=self.waitForCommands)
        t.start()

    def sendUpdate(self,pcaId,comment=None):
        partition = self.PCAs[pcaId]
        data = {
            "id": self.MyId,
            "state": self.StateMachineForPca[partition.id].currentState
        }
        if pcaId in self.pcaConfigTag:
            data["tag"] = self.pcaConfigTag[pcaId]
        if comment:
            data["comment"] = comment
        try:
            socketSendUpdateToPCA = self.context.socket(zmq.REQ)
            socketSendUpdateToPCA.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
            socketSendUpdateToPCA.setsockopt(zmq.LINGER,0)
            socketSendUpdateToPCA.connect("tcp://%s:%s" % (partition.address,partition.portUpdates))
            socketSendUpdateToPCA.send(json.dumps(data).encode())
            r = socketSendUpdateToPCA.recv()
            if r == codes.idUnknown:
                print("Partition does not know %s bad,very bad" % self.MyId)
        except zmq.Again:
            print("timeout sending status")
        except zmq.error.ContextTerminated:
            pass
        except Exception as e:
            print("error sending status: %s" % str(e))
        finally:
            socketSendUpdateToPCA.close()

    def addPartition(self,partitionData):
        self.PCAs[partitionData.id] = partitionData
        self.StateMachineForPca[partitionData.id] = Statemachine(self.StateMachineFile,"Unconfigured")
        self.isPCAinTransition[partitionData.id] = False

    def deletePartition(self,pcaId):
        del self.PCAs[pcaId]
        del self.StateMachineForPca[pcaId]
        del self.isPCAinTransition[pcaId]
        if pcaId in self.pcaConfigTag:
            del self.pcaConfigTag[pcaId]

    def handleCommonCommands(self,message):
        """handles command common for all Global Systems; returns False if command is unknown"""
        command = message[0]
        if command == codes.ping:
            self.commandSocket.send(codes.ok)
            return True
        elif command == codes.pcaAsksForDetectorStatus:
            pcaId = message[1].decode()
            if pcaId and pcaId in self.PCAs:
                if pcaId in self.pcaConfigTag:
                    self.commandSocket.send_multipart([self.StateMachineForPca[pcaId].currentState.encode(),self.pcaConfigTag[pcaId].encode()])
                else:
                    self.commandSocket.send_multipart([self.StateMachineForPca[pcaId].currentState.encode()])
            return True
        elif command == codes.addPartition:
            data = partitionDataObject(json.loads(message[1].decode()))
            self.addPartition(data)
            self.commandSocket.send(codes.ok)
            return True
        elif command == codes.deletePartition:
            pcaId = message[1].decode()
            self.deletePartition(pcaId)
            self.commandSocket.send(codes.ok)
            return True
        elif command == codes.remapDetector:
            detectorId = message[2].decode()
            if message[1] == codes.removed:
                self.abortFunction(self.detectorMapping[detectorId])
                del self.detectorMapping[detectorId]
            else:
                pcaId = message[1].decode()
                self.abortFunction(pcaId)
                if detectorId in self.detectorMapping:
                    self.abortFunction(self.detectorMapping[detectorId])
                self.detectorMapping[detectorId] = pcaId
            self.commandSocket.send(codes.ok)
            return True
        return False

    def sendUpdateToAll(self,comment=None):
        for pca in self.PCAs:
            self.sendUpdate(pca)

    def abortAll(self):
        for partition in self.PCAs:
            self.abortFunction(partition.id)

    def transition(self,pcaId,command,tag=None):
        self.StateMachineForPca[pcaId].transition(command)
        if tag:
            self.pcaConfigTag[pcaId] = tag
        else:
            if pcaId in self.pcaConfigTag:
                del self.pcaConfigTag[pcaId]

    def executeScript(self,scriptname):
        self.scriptProcess = subprocess.Popen(["exec ./"+scriptname], shell=True)
        ret = self.scriptProcess.wait()
        if self.abort:
            return
        if ret:
            return False
        else:
            return True

    def terminate(self):
        self.context.term()
        self.abort = True
        if self.scriptProcess:
            self.scriptProcess.terminate()
class DCSClient(GlobalSystemClient):

    def __init__(self):
        super().__init__("DCS")


    def waitForCommands(self):
        while True:
            try:
                message = self.commandSocket.recv_multipart()
                if self.handleCommonCommands(message):
                    continue
                pcaId = None
                tag = None
                if len(message) > 1:
                    pcaId = message[1].decode()
                if len(message) > 2:
                    tag = message[2].decode()
                command = message[0].decode()
                if command == DCSTransitions.configure:
                    if self.StateMachineForPca[pcaId].currentState not in {DCSStates.Active,DCSStates.Unconfigured}:
                        self.commandSocket.send(codes.busy)
                        continue
                    elif not self.StateMachineForPca[pcaId].checkIfPossible(DCSTransitions.configure) or not tag or not pcaId:
                        self.commandSocket.send(codes.error)
                        print("error")
                        continue
                    else:
                        self.commandSocket.send(codes.ok)
                        self.transition(pcaId,DCSTransitions.configure,tag)
                        self.isPCAinTransition[pcaId] = True
                        self.sendUpdate(pcaId)
                        workThread = threading.Thread(name="worker", target=self.configure, args=(pcaId,tag))
                        workThread.start()
                        continue
                if command == DCSTransitions.abort:
                    if pcaId and pcaId in self.PCAs:
                        self.abortFunction(pcaId)
                        self.commandSocket.send(codes.ok)
                    else:
                        self.commandSocket.send(codes.error)
                    continue
                self.commandSocket.send(codes.unknownCommand)
            except zmq.error.ContextTerminated:
                self.commandSocket.close()
                break

    def configure(self,pcaId,tag):
        ret = self.executeScript("detectorScript.sh")
        if ret:
            if self.abort:
                self.abort = False
                self.transition(pcaId,DCSTransitions.abort)
                self.isPCAinTransition[pcaId] = False
                self.sendUpdate(pcaId,"transition aborted")
                return
            self.transition(pcaId,DCSTransitions.success,tag)
            self.isPCAinTransition[pcaId] = False
            self.sendUpdate(pcaId)
        else:
            self.transition(pcaId,DCSTransitions.error)
            self.sendUpdate(pcaId,"transition failed")
            self.isPCAinTransition[pcaId] = False

    def abortFunction(self,pcaId):
        #terminate Transition if active
        if self.isPCAinTransition[pcaId]:
            self.abort = True
            self.scriptProcess.terminate()
        else:
            self.transition(pcaId,DCSTransitions.abort)
            self.sendUpdate(pcaId)

    def waitForDCSMessages(self,message):
        detId,message = message
        if detId not in self.detectorMapping:
            print("id unknown")
            return
        pcaId = self.detectorMapping[detId]
        partition = self.PCAs[pcaId]
        data = {
            "id": "DCS",
            "detectorId": detId,
            "message": message,
        }
        data=json.dumps(data).encode()
        try:
            socketSendUpdateToPCA = self.context.socket(zmq.REQ)
            socketSendUpdateToPCA.connect("tcp://%s:%s" % (partition.address,partition.portUpdates))
            socketSendUpdateToPCA.send(data)
            r = socketSendUpdateToPCA.recv()
            if r == codes.idUnknown:
                print("Partition does not know DCS bad,very bad")
        except zmq.Again:
            print("timeout sending status")
        except zmq.error.ContextTerminated:
            pass
        except Exception as e:
            print("error sending DCS message: %s" % str(e))
        finally:
            socketSendUpdateToPCA.close()
class TFCClient(GlobalSystemClient):

    def __init__(self):
        super().__init__("TFC")


    def waitForCommands(self):
        while True:
            try:
                message = self.commandSocket.recv_multipart()
                if self.handleCommonCommands(message):
                    continue
                pcaId = None
                tag = None
                if len(message) > 1:
                    pcaId = message[1].decode()
                if len(message) > 2:
                    tag = message[2].decode()
                command = message[0].decode()
                if command == TFCTransitions.configure:
                    if self.StateMachineForPca[pcaId].currentState not in {TFCStates.Active,TFCStates.Unconfigured}:
                        self.commandSocket.send(codes.busy)
                        continue
                    elif not self.StateMachineForPca[pcaId].checkIfPossible(TFCTransitions.configure) or not tag or not pcaId:
                        self.commandSocket.send(codes.error)
                        print("error")
                        continue
                    else:
                        self.commandSocket.send(codes.ok)
                        self.transition(pcaId,TFCTransitions.configure,tag)
                        self.isPCAinTransition[pcaId] = True
                        self.sendUpdate(pcaId)
                        workThread = threading.Thread(name="worker", target=self.configure, args=(pcaId,tag))
                        workThread.start()
                        continue
                if command == TFCTransitions.abort:
                    if pcaId and pcaId in self.PCAs:
                        self.abortFunction(pcaId)
                        self.commandSocket.send(codes.ok)
                    else:
                        self.commandSocket.send(codes.error)
                    continue
                self.commandSocket.send(codes.unknownCommand)
            except zmq.error.ContextTerminated:
                self.commandSocket.close()
                break

    def configure(self,pcaId,tag):
        ret = self.executeScript("detectorScript.sh")
        if ret:
            if self.abort:
                self.abort = False
                self.transition(pcaId,TFCTransitions.abort)
                self.isPCAinTransition[pcaId] = False
                self.sendUpdate(pcaId,"transition aborted")
                return
            self.transition(pcaId,TFCTransitions.success,tag)
            self.isPCAinTransition[pcaId] = False
            self.sendUpdate(pcaId)
        else:
            self.transition(pcaId,TFCTransitions.error)
            self.sendUpdate(pcaId,"transition failed")
            self.isPCAinTransition[pcaId] = False

    def abortFunction(self,pcaId):
        #terminate Transition if active
        if self.isPCAinTransition[pcaId]:
            self.abort = True
            self.scriptProcess.terminate()
        else:
            self.transition(pcaId,TFCTransitions.abort)
            self.sendUpdate(pcaId)

class QAClient(GlobalSystemClient):
    def __init__(self):
        super().__init__("QA")


    def waitForCommands(self):
        while True:
            try:
                message = self.commandSocket.recv_multipart()
                if self.handleCommonCommands(message):
                    continue
                pcaId = None
                tag = None
                if len(message) > 1:
                    pcaId = message[1].decode()
                if len(message) > 2:
                    tag = message[2].decode()
                command = message[0].decode()
                if command == QATransitions.configure:
                    if self.StateMachineForPca[pcaId].currentState not in {QAStates.Active,QAStates.Unconfigured}:
                        self.commandSocket.send(codes.busy)
                        continue
                    elif not self.StateMachineForPca[pcaId].checkIfPossible(QATransitions.configure) or not tag or not pcaId:
                        self.commandSocket.send(codes.error)
                        print("error")
                        continue
                    else:
                        self.commandSocket.send(codes.ok)
                        self.transition(pcaId,QATransitions.configure,tag)
                        self.isPCAinTransition[pcaId] = True
                        self.sendUpdate(pcaId)
                        workThread = threading.Thread(name="worker", target=self.configure, args=(pcaId,tag))
                        workThread.start()
                        continue
                if command == QATransitions.abort:
                    if pcaId and pcaId in self.PCAs:
                        self.abortFunction(pcaId)
                        self.commandSocket.send(codes.ok)
                    else:
                        self.commandSocket.send(codes.error)
                    continue
                if command == QATransitions.start:
                    if pcaId and pcaId in self.PCAs:
                        self.commandSocket.send(codes.ok)
                        self.transition(pcaId,QATransitions.start,self.pcaConfigTag[pcaId])
                        self.sendUpdate(pcaId)
                    else:
                        self.commandSocket.send(codes.error)
                    continue
                if command == QATransitions.stop:
                    if pcaId and pcaId in self.PCAs:
                        self.commandSocket.send(codes.ok)
                        self.transition(pcaId,QATransitions.stop,self.pcaConfigTag[pcaId])
                        self.sendUpdate(pcaId)
                    else:
                        self.commandSocket.send(codes.error)
                    continue
                self.commandSocket.send(codes.unknownCommand)
            except zmq.error.ContextTerminated:
                self.commandSocket.close()
                break

    def configure(self,pcaId,tag):
        ret = self.executeScript("detectorScript.sh")
        if ret:
            if self.abort:
                self.abort = False
                self.transition(pcaId,QATransitions.abort)
                self.isPCAinTransition[pcaId] = False
                self.sendUpdate(pcaId,"transition aborted")
                return
            self.transition(pcaId,QATransitions.success,tag)
            self.isPCAinTransition[pcaId] = False
            self.sendUpdate(pcaId)
        else:
            self.transition(pcaId,QATransitions.error)
            self.sendUpdate(pcaId,"transition failed")
            self.isPCAinTransition[pcaId] = False

    def abortFunction(self,pcaId):
        #terminate Transition if active
        if self.isPCAinTransition[pcaId]:
            self.abort = True
            self.scriptProcess.terminate()
        else:
            self.transition(pcaId,QATransitions.abort)
            self.sendUpdate(pcaId)

    def waitForQAMessages(self,message):
        detId,message = message
        if detId not in self.detectorMapping:
            print("id unknown")
            return
        pcaId = self.detectorMapping[detId]
        partition = self.PCAs[pcaId]
        data = {
            "id": "QA",
            "detectorId": detId,
            "message": message,
        }
        data=json.dumps(data).encode()
        try:
            socketSendUpdateToPCA = self.context.socket(zmq.REQ)
            socketSendUpdateToPCA.connect("tcp://%s:%s" % (partition.address,partition.portUpdates))
            socketSendUpdateToPCA.send(data)
            r = socketSendUpdateToPCA.recv()
            if r == codes.idUnknown:
                print("Partition does not know QA bad,very bad")
        except zmq.Again:
            print("timeout sending status")
        except zmq.error.ContextTerminated:
            pass
        except Exception as e:
            print("error sending QA message: %s" % str(e))
        finally:
            socketSendUpdateToPCA.close()

class FLESClient(GlobalSystemClient):
    def __init__(self):
        self.jobIds = {}
        super().__init__("FLES")


    def waitForCommands(self):
        while True:
            try:
                message = self.commandSocket.recv_multipart()
                if self.handleCommonCommands(message):
                    continue
                pcaId = None
                tag = None
                if len(message) > 1:
                    pcaId = message[1].decode()
                if len(message) > 2:
                    tag = message[2].decode()
                command = message[0].decode()
                if command == FLESTransitions.configure:
                    if self.StateMachineForPca[pcaId].currentState not in {FLESStates.Active,FLESStates.Unconfigured}:
                        self.commandSocket.send(codes.busy)
                        continue
                    elif not self.StateMachineForPca[pcaId].checkIfPossible(FLESTransitions.configure) or not tag or not pcaId:
                        self.commandSocket.send(codes.error)
                        print("error")
                        continue
                    else:
                        self.commandSocket.send(codes.ok)
                        self.transition(pcaId,FLESTransitions.configure,tag)
                        self.isPCAinTransition[pcaId] = True
                        self.sendUpdate(pcaId)
                        self.configure(pcaId,tag)
                        #workThread = threading.Thread(name="worker", target=self.configure, args=(pcaId,tag))
                        #workThread.start()
                        continue
                if command == FLESTransitions.abort:
                    if pcaId and pcaId in self.PCAs:
                        self.abortFunction(pcaId)
                        self.commandSocket.send(codes.ok)
                    else:
                        self.commandSocket.send(codes.error)
                    continue
                if command == FLESTransitions.start:
                    if pcaId and pcaId in self.PCAs:
                        self.commandSocket.send(codes.ok)
                        self.transition(pcaId,FLESTransitions.start,self.pcaConfigTag[pcaId])
                        self.sendUpdate(pcaId)
                    else:
                        self.commandSocket.send(codes.error)
                    continue
                if command == FLESTransitions.stop:
                    if pcaId and pcaId in self.PCAs:
                        self.commandSocket.send(codes.ok)
                        self.transition(pcaId,FLESTransitions.stop,self.pcaConfigTag[pcaId])
                        self.sendUpdate(pcaId)
                    else:
                        self.commandSocket.send(codes.error)
                    continue
                self.commandSocket.send(codes.unknownCommand)
            except zmq.error.ContextTerminated:
                self.commandSocket.close()
                break

    def checkIfRunning(self,jobId,pcaId):
        ret = "RUNNING"
        while ret=="RUNNING":
            time.sleep(2)
            ret = subprocess.check_output(["scontrol show job "+jobId], shell=True)
            ret = re.search("JobState=(\w+)",str(ret)).group(1)
        self.abortFunction(pcaId)


    def executeScript(self,scriptname):
        #self.scriptProcess = subprocess.check_output(["exec ./"+scriptname], shell=True)
        try:
            ret = subprocess.check_output([scriptname], shell=True)
            ret = re.search("job (\d+)",str(ret)).group(1)
        except:
            ret = False
        return ret

    def configure(self,pcaId,tag):
        ret = self.executeScript("bash -c 'cd ~/flesnet; ./start_readout readout.cfg'")
        self.jobIds[pcaId] = ret
        print(ret)
        workThread = threading.Thread(name="worker", target=self.checkIfRunning, args=(ret,pcaId))
        workThread.start()
        if ret:
            if self.abort:
                self.abort = False
                self.transition(pcaId,FLESTransitions.abort)
                self.isPCAinTransition[pcaId] = False
                self.sendUpdate(pcaId,"transition aborted")
                return
            self.transition(pcaId,FLESTransitions.success,tag)
            self.isPCAinTransition[pcaId] = False
            self.sendUpdate(pcaId)
        else:
            self.transition(pcaId,FLESTransitions.error)
            self.sendUpdate(pcaId,"transition failed")
            self.isPCAinTransition[pcaId] = False

    def abortFunction(self,pcaId):
        #terminate Transition if active
        if pcaId in self.jobIds:
            subprocess.check_output(["scancel "+self.jobIds[pcaId]], shell=True)
        if self.isPCAinTransition[pcaId]:
            self.abort = True
            #self.scriptProcess.terminate()
        else:
            self.transition(pcaId,FLESTransitions.abort)
            self.sendUpdate(pcaId)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("please enter a SystemType (TFC,DCS,QA,FLES,all)")
        sys.exit(1)
    elif "all" in sys.argv:
        TFCClient()
        DCSClient()
        QAClient()
        FLESClient()
        x = input()
        sys.exit(0)
    dcs = None
    if "TFC" in sys.argv:
        test = TFCClient()
    if "DCS" in sys.argv:
        dcs = DCSClient()
    if "QA" in sys.argv:
        test = QAClient()
    if "FLES" in sys.argv:
        test = FLESClient()
    if dcs:
        while True:
            try:
                x = input()
                message = x.split()
                dcs.waitForDCSMessages(message)
            except KeyboardInterrupt:
                dcs.terminate()
                break
            except EOFError:
                time.sleep(500000)
                continue
    x = input()
