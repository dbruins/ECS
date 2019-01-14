#!/usr/bin/python3
from DataObjects import DataObjectCollection, globalSystemDataObject, mappingDataObject, partitionDataObject
from states import GlobalSystemTransitions,DCSStates,DCSTransitions, FLESStates, FLESTransitions, QAStates, QATransitions, TFCStates, TFCTransitions
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
import os
import errno

class GlobalSystemControler:
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
        self.context.setsockopt(zmq.LINGER,0)
        self.scriptProcess = None
        self.abort = False

        mapping = None
        #ask ECS For required Data
        while mapping == None:
            try:
                requestSocket = self.context.socket(zmq.REQ)
                requestSocket.connect("tcp://%s:%s" % (self.conf['ECSAddress'],self.conf['ECSRequestPort']))
                requestSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)

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

        #holds information which detector belongs to which Partition
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
        self.pcaSequenceNumber = {}
        self.pcaStatemachineLock = {}
        for p in partitions:
            self.PCAs[p.id] = p
            self.StateMachineForPca[p.id] = Statemachine(self.StateMachineFile,"Unconfigured")
            self.isPCAinTransition[p.id] = False
            self.pcaSequenceNumber[p.id] = 0
            self.pcaStatemachineLock[p.id] = threading.Lock()

        self.commandSocket = self.context.socket(zmq.REP)
        self.commandSocket.bind("tcp://*:%i" % globalSystemInfo.portCommand)

        #send update to All PCAs
        self.sendUpdateToAll()

        t = threading.Thread(name="waitForCommands", target=self.waitForCommands)
        t.start()

        t = threading.Thread(name="waitForPipeMessages", target=self.waitForPipeMessages)
        t.start()

    def waitForPipeMessages(self):
        try:
            os.mkfifo("pipe"+self.MyId)
        except OSError as oe:
            if oe.errno != errno.EEXIST:
                raise
        while True:
            with open("pipe"+self.MyId) as fifo:
                while True:
                    message = fifo.read().replace('\n', '')
                    if len(message) == 0:
                        #pipe closed
                        break
                    self.handleSystemMessage(message)

    def sendUpdate(self,pcaId,sequenceNumber,comment=None):
        """send current status update to a Parition with id pcaId"""
        partition = self.PCAs[pcaId]
        data = {
            "id": self.MyId,
            "state": self.StateMachineForPca[partition.id].currentState,
            "sequenceNumber": sequenceNumber,
        }
        if pcaId in self.pcaConfigTag:
            data["tag"] = self.pcaConfigTag[pcaId]
        if comment:
            data["comment"] = comment
        #repeat until update is received
        while True:
            try:
                socketSendUpdateToPCA = self.context.socket(zmq.REQ)
                socketSendUpdateToPCA.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
                socketSendUpdateToPCA.connect("tcp://%s:%s" % (partition.address,partition.portUpdates))
                socketSendUpdateToPCA.send(json.dumps(data).encode())
                r = socketSendUpdateToPCA.recv()
                #success
                return
            except zmq.Again:
                print("timeout sending status %s" % pcaId)
                if sequenceNumber < self.pcaSequenceNumber[pcaId]:
                    #if there is already a new update give up
                    return
            except zmq.error.ContextTerminated:
                return
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
        elif command == codes.pcaAsksForDetectorStatus:
            pcaId = message[1].decode()
            if pcaId and pcaId in self.PCAs:
                if pcaId in self.pcaConfigTag:
                    self.commandSocket.send_multipart([self.StateMachineForPca[pcaId].currentState.encode(),self.pcaConfigTag[pcaId].encode()])
                else:
                    self.commandSocket.send_multipart([self.StateMachineForPca[pcaId].currentState.encode()])
        elif command == codes.addPartition:
            data = partitionDataObject(json.loads(message[1].decode()))
            self.addPartition(data)
            self.commandSocket.send(codes.ok)
        elif command == codes.deletePartition:
            pcaId = message[1].decode()
            self.deletePartition(pcaId)
            self.commandSocket.send(codes.ok)
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
        elif command.decode() == GlobalSystemTransitions.reset:
            pcaId = message[1].decode()
            self.reset(pcaId)
            self.commandSocket.send(codes.ok)
        else:
            #command unknown
            return False
        return True

    def sendUpdateToAll(self,comment=None):
        for pca in self.PCAs:
            t = threading.Thread(name='update'+str(0), target=self.sendUpdate, args=(pca,0,))
            t.start()
            #self.sendUpdate(pca,0)

    def abortAll(self):
        for partition in self.PCAs:
            self.abortFunction(partition.id)

    def transition(self,pcaId,transition,tag=None,comment=None):
        try:
            self.pcaStatemachineLock[pcaId].acquire()
            if self.StateMachineForPca[pcaId].transition(transition):
                if tag:
                    self.pcaConfigTag[pcaId] = tag
                else:
                    if pcaId in self.pcaConfigTag:
                        del self.pcaConfigTag[pcaId]
                self.pcaSequenceNumber[pcaId] = self.pcaSequenceNumber[pcaId]+1
                sequenceNumber = self.pcaSequenceNumber[pcaId]
                self.pcaStatemachineLock[pcaId].release()
                #self.sendUpdate(pcaId,sequenceNumber,comment)
                t = threading.Thread(name='update'+str(0), target=self.sendUpdate, args=(pcaId,sequenceNumber,comment))
                t.start()
        finally:
            if self.pcaStatemachineLock[pcaId].locked():
                self.pcaStatemachineLock[pcaId].release()

    def executeScript(self,scriptname):
        self.scriptProcess = subprocess.Popen(["exec ./"+scriptname], shell=True)
        ret = self.scriptProcess.wait()
        if self.abort:
            return
        if ret:
            return False
        else:
            return True


    def error(self,pcaId=None):
        if pcaId:
            self.transition(pcaId,GlobalSystemTransitions.error)
        else:
            for partition in self.PCAs:
                self.transition(partition,GlobalSystemTransitions.error)
                if self.isPCAinTransition[partition]:
                    self.abort = True
                    self.scriptProcess.terminate()

    def resolved(self,pcaId=None):
        if pcaId:
            self.transition(pcaId,GlobalSystemTransitions.resolved)
        else:
            for partition in self.PCAs:
                self.transition(partition,GlobalSystemTransitions.resolved)

    def reset(self,pcaId=None):
        if pcaId:
            self.transition(pcaId,GlobalSystemTransitions.reset)
        else:
            for partition in self.PCAs:
                self.transition(partition,GlobalSystemTransitions.reset)

    def handleSystemMessage(self,message):
        if message == "error":
            self.error()
        elif message == "resolved":
            self.resolved()
        else:
            print('received unknown message via pipe: %s' % (message,))

    def terminate(self):
        self.context.term()
        self.abort = True
        if self.scriptProcess:
            self.scriptProcess.terminate()
class DCSControler(GlobalSystemControler):

    def __init__(self):
        super().__init__("DCS")

    def waitForCommands(self):
        """wait for command on the command socket"""
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
                self.transition(pcaId,DCSTransitions.abort,comment="transition aborted")
                self.isPCAinTransition[pcaId] = False
                return
            self.transition(pcaId,DCSTransitions.success,tag)
            self.isPCAinTransition[pcaId] = False
        else:
            self.transition(pcaId,DCSTransitions.error,comment="transition failed")
            self.isPCAinTransition[pcaId] = False

    def abortFunction(self,pcaId):
        #terminate Transition if active
        if self.isPCAinTransition[pcaId]:
            self.abort = True
            self.scriptProcess.terminate()
        else:
            self.transition(pcaId,DCSTransitions.abort)

    def handleSystemMessage(self,data):
        message = data.split()[0]
        if message == "detectorError":
            message,detId = data.split()
            pcaId = self.detectorMapping[detId]
            partition = self.PCAs[pcaId]
            data = {
                "id": "DCS",
                "detectorId": detId,
                "message": message,
            }
            self.sendDCSMessage(data,partition)
        else:
            super().handleSystemMessage(message)

    def sendDCSMessage(self,data,partition):
        data=json.dumps(data).encode()
        try:
            socketSendUpdateToPCA = self.context.socket(zmq.REQ)
            socketSendUpdateToPCA.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
            socketSendUpdateToPCA.connect("tcp://%s:%s" % (partition.address,partition.portCommand))
            socketSendUpdateToPCA.send_multipart([codes.subsystemMessage,data])
            r = socketSendUpdateToPCA.recv()
            if r == codes.idUnknown:
                print("Partition does not know DCS")
        except zmq.Again:
            print("timeout sending DCS message")
        except zmq.error.ContextTerminated:
            pass
        except Exception as e:
            print("error sending DCS message: %s" % str(e))
        finally:
            socketSendUpdateToPCA.close()
class TFCControler(GlobalSystemControler):

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
                self.transition(pcaId,TFCTransitions.abort,comment="transition aborted")
                self.isPCAinTransition[pcaId] = False
                return
            self.transition(pcaId,TFCTransitions.success,tag)
            self.isPCAinTransition[pcaId] = False
        else:
            self.transition(pcaId,TFCTransitions.error,comment="transition failed")
            self.isPCAinTransition[pcaId] = False

    def abortFunction(self,pcaId):
        #terminate Transition if active
        if self.isPCAinTransition[pcaId]:
            self.abort = True
            self.scriptProcess.terminate()
        else:
            self.transition(pcaId,TFCTransitions.abort)

class QAControler(GlobalSystemControler):
    def __init__(self):
        super().__init__("QA")


    def waitForCommands(self):
        """wait for command on the command socket"""
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
                    else:
                        self.commandSocket.send(codes.error)
                    continue
                if command == QATransitions.stop:
                    if pcaId and pcaId in self.PCAs:
                        self.commandSocket.send(codes.ok)
                        self.transition(pcaId,QATransitions.stop,self.pcaConfigTag[pcaId])
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
                self.transition(pcaId,QATransitions.abort,comment="transition aborted")
                self.isPCAinTransition[pcaId] = False
                return
            self.transition(pcaId,QATransitions.success,tag)
            self.isPCAinTransition[pcaId] = False
        else:
            self.transition(pcaId,QATransitions.error,comment="transition failed")
            self.isPCAinTransition[pcaId] = False

    def abortFunction(self,pcaId):
        #terminate Transition if active
        if self.isPCAinTransition[pcaId]:
            self.abort = True
            self.scriptProcess.terminate()
        else:
            self.transition(pcaId,QATransitions.abort)

    def handleSystemMessage(self,data):
        message = data.split()[0]
        if message == "QAError":
            message,detID = data.split()
            pcaId = self.detectorMapping[detId]
            partition = self.PCAs[pcaId]
            data = {
                "id": "DCS",
                "detectorId": detId,
                "message": message,
            }
            self.sendQAMessage(data,partition)
        else:
            super().handleSystemMessage(message)

    def sendQAMessage(self,message,partition):
        try:
            data=json.dumps(data).encode()
            socketSendUpdateToPCA = self.context.socket(zmq.REQ)
            socketSendUpdateToPCA.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
            socketSendUpdateToPCA.connect("tcp://%s:%s" % (partition.address,partition.portUpdates))
            socketSendUpdateToPCA.send(data)
            r = socketSendUpdateToPCA.recv()
            if r == codes.idUnknown:
                print("Partition does not know QA")
        except zmq.Again:
            print("timeout sending QA message")
        except zmq.error.ContextTerminated:
            pass
        except Exception as e:
            print("error sending QA message: %s" % str(e))
        finally:
            socketSendUpdateToPCA.close()

class FLESControler(GlobalSystemControler):
    def __init__(self):
        self.jobIds = {}
        super().__init__("FLES")


    def waitForCommands(self):
        """wait for command on the command socket"""
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
                    else:
                        self.commandSocket.send(codes.error)
                    continue
                if command == FLESTransitions.stop:
                    if pcaId and pcaId in self.PCAs:
                        self.commandSocket.send(codes.ok)
                        self.transition(pcaId,FLESTransitions.stop,self.pcaConfigTag[pcaId])
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
                self.transition(pcaId,QATransitions.abort,comment="transition aborted")
                self.isPCAinTransition[pcaId] = False
                return
            self.transition(pcaId,QATransitions.success,tag)
            self.isPCAinTransition[pcaId] = False
        else:
            self.transition(pcaId,QATransitions.error,comment="transition failed")
            self.isPCAinTransition[pcaId] = False

    def abortFunction(self,pcaId):
        #terminate Transition if active
        if self.isPCAinTransition[pcaId]:
            self.abort = True
            self.scriptProcess.terminate()
        else:
            self.transition(pcaId,QATransitions.abort)
"""
    def checkIfRunning(self,jobId,pcaId):
        ret = "RUNNING"
        while ret=="RUNNING":
            time.sleep(2)
            ret = subprocess.check_output(["scontrol show job "+jobId], shell=True)
            ret = re.search("JobState=(\w+)",str(ret)).group(1)
        self.abortFunction(pcaId)


    def executeScript(self,scriptname):
        #self.scriptProcess = subprocess.check_output(["exec ./"+scriptname], shell=True)
        ret = subprocess.check_output([scriptname], shell=True)
        ret = re.search("job (\d+)",str(ret))
        if ret == None:
            return False
        return ret.group(1)

    def configure(self,pcaId,tag):
        ret = self.executeScript("bash -c 'cd ~/flesnet; ./startTest readoutTest.cfg'")
        print(ret)
        if ret:
            self.jobIds[pcaId] = ret
            workThread = threading.Thread(name="worker", target=self.checkIfRunning, args=(ret,pcaId))
            workThread.start()
            if self.abort:
                self.abort = False
                self.transition(pcaId,FLESTransitions.abort,comment="transition aborted")
                self.isPCAinTransition[pcaId] = False
                return
            self.transition(pcaId,FLESTransitions.success,tag)
            self.isPCAinTransition[pcaId] = False
        else:
            self.transition(pcaId,FLESTransitions.error,comment="transition failed")
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

"""
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("please enter a SystemType (TFC,DCS,QA,FLES,all)")
        sys.exit(1)
    elif "all" in sys.argv:
        TFCControler()
        DCSControler()
        QAControler()
        FLESControler()
        x = input()
        sys.exit(0)
    dcs = None
    if "TFC" in sys.argv:
        test = TFCControler()
    if "DCS" in sys.argv:
        dcs = DCSControler()
    if "QA" in sys.argv:
        test = QAControler()
    if "FLES" in sys.argv:
        test = FLESControler()
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
