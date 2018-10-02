#!/usr/bin/python3
from DataObjects import DataObjectCollection, globalSystemDataObject, mappingDataObject, partitionDataObject
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
                print(globalSystemInfo)

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
        self.StateMachineForPca = {}
        self.PCAs = {}
        self.isPCAinTransition = {}
        self.pcaConfigTag = {}
        for p in partitions:
            self.PCAs[p.id] = p
            self.StateMachineForPca[p.id] = Statemachine(configDet["stateFile"],"Unconfigured")
            self.isPCAinTransition[p.id] = False

        self.commandSocket = self.context.socket(zmq.REP)
        self.commandSocket.bind("tcp://*:%i" % globalSystemInfo.portCommand)

        #send update to All PCAs
        self.sendUpdateToAll()

        _thread.start_new_thread(self.waitForCommands,())
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
        self.scriptProcess = subprocess.Popen(["exec sh "+scriptname], shell=True)
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
                command = self.commandSocket.recv_multipart()
                pcaId = None
                tag = None
                if len(command) > 1:
                    pcaId = command[1].decode()
                if len(command) > 2:
                    tag = command[2].decode()
                command = command[0]
                if command == codes.ping:
                    self.commandSocket.send(codes.ok)
                    continue
                if command == codes.pcaAsksForDetectorStatus:
                    if pcaId and pcaId in self.PCAs:
                        if pcaId in self.pcaConfigTag:
                            self.commandSocket.send_multipart([self.StateMachineForPca[pcaId].currentState.encode(),self.pcaConfigTag[pcaId].encode()])
                        else:
                            self.commandSocket.send_multipart([self.StateMachineForPca[pcaId].currentState.encode()])
                    continue
                command=command.decode()
                print(command)
                if command == "configure":
                    if self.StateMachineForPca[pcaId].currentState not in {"Active","Unconfigured"}:
                        self.commandSocket.send(codes.busy)
                        continue
                    elif not self.StateMachineForPca[pcaId].checkIfPossible("configure") or not tag or not pcaId:
                        self.commandSocket.send(codes.error)
                        print("error")
                        continue
                    else:
                        self.commandSocket.send(codes.ok)
                        self.transition(pcaId,"configure",tag)
                        self.isPCAinTransition[pcaId] = True
                        self.sendUpdate(pcaId)
                        workThread = threading.Thread(name="worker", target=self.configure, args=(pcaId,tag))
                        workThread.start()
                        continue
                if command == "abort":
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
        print(ret)
        if ret:
            if self.abort:
                self.abort = False
                self.transition(pcaId,"abort")
                self.isPCAinTransition[pcaId] = False
                self.sendUpdate(pcaId,"transition aborted")
                return
            self.transition(pcaId,"success",tag)
            self.isPCAinTransition[pcaId] = False
            self.sendUpdate(pcaId)
        else:
            self.transition(pcaId,"error")
            self.sendUpdate(pcaId,"transition failed")
            self.isPCAinTransition[pcaId] = False

    def abortFunction(self,pcaId):
        #terminate Transition if active
        if self.isPCAinTransition[pcaId]:
            self.abort = True
            self.scriptProcess.terminate()
        else:
            self.transition(pcaId,"abort")
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
            print(data)
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
                command = self.commandSocket.recv_multipart()
                pcaId = None
                tag = None
                if len(command) > 1:
                    pcaId = command[1].decode()
                if len(command) > 2:
                    tag = command[2].decode()
                command = command[0]
                if command == codes.ping:
                    self.commandSocket.send(codes.ok)
                    continue
                if command == codes.pcaAsksForDetectorStatus:
                    if pcaId and pcaId in self.PCAs:
                        if pcaId in self.pcaConfigTag:
                            self.commandSocket.send_multipart([self.StateMachineForPca[pcaId].currentState.encode(),self.pcaConfigTag[pcaId].encode()])
                        else:
                            self.commandSocket.send_multipart([self.StateMachineForPca[pcaId].currentState.encode()])
                    continue
                command=command.decode()
                print(command)
                if command == "configure":
                    if self.StateMachineForPca[pcaId].currentState not in {"Active","Unconfigured"}:
                        self.commandSocket.send(codes.busy)
                        continue
                    elif not self.StateMachineForPca[pcaId].checkIfPossible("configure") or not tag or not pcaId:
                        self.commandSocket.send(codes.error)
                        print("error")
                        continue
                    else:
                        self.commandSocket.send(codes.ok)
                        self.transition(pcaId,"configure",tag)
                        self.isPCAinTransition[pcaId] = True
                        self.sendUpdate(pcaId)
                        workThread = threading.Thread(name="worker", target=self.configure, args=(pcaId,tag))
                        workThread.start()
                        continue
                if command == "abort":
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
                self.transition(pcaId,"abort")
                self.isPCAinTransition[pcaId] = False
                self.sendUpdate(pcaId,"transition aborted")
                return
            self.transition(pcaId,"success",tag)
            self.isPCAinTransition[pcaId] = False
            self.sendUpdate(pcaId)
        else:
            self.transition(pcaId,"error")
            self.sendUpdate(pcaId,"transition failed")
            self.isPCAinTransition[pcaId] = False

    def abortFunction(self,pcaId):
        #terminate Transition if active
        if self.isPCAinTransition[pcaId]:
            self.abort = True
            self.scriptProcess.terminate()
        else:
            self.transition(pcaId,"abort")
            self.sendUpdate(pcaId)

class QAClient(GlobalSystemClient):
    def __init__(self):
        super().__init__("QA")


    def waitForCommands(self):
        while True:
            try:
                command = self.commandSocket.recv_multipart()
                pcaId = None
                tag = None
                if len(command) > 1:
                    pcaId = command[1].decode()
                if len(command) > 2:
                    tag = command[2].decode()
                command = command[0]
                if command == codes.ping:
                    self.commandSocket.send(codes.ok)
                    continue
                if command == codes.pcaAsksForDetectorStatus:
                    if pcaId and pcaId in self.PCAs:
                        if pcaId in self.pcaConfigTag:
                            self.commandSocket.send_multipart([self.StateMachineForPca[pcaId].currentState.encode(),self.pcaConfigTag[pcaId].encode()])
                        else:
                            self.commandSocket.send_multipart([self.StateMachineForPca[pcaId].currentState.encode()])
                    continue
                command=command.decode()
                print(command)
                if command == "configure":
                    if self.StateMachineForPca[pcaId].currentState not in {"Active","Unconfigured"}:
                        self.commandSocket.send(codes.busy)
                        continue
                    elif not self.StateMachineForPca[pcaId].checkIfPossible("configure") or not tag or not pcaId:
                        self.commandSocket.send(codes.error)
                        print("error")
                        continue
                    else:
                        self.commandSocket.send(codes.ok)
                        self.transition(pcaId,"configure",tag)
                        self.isPCAinTransition[pcaId] = True
                        self.sendUpdate(pcaId)
                        workThread = threading.Thread(name="worker", target=self.configure, args=(pcaId,tag))
                        workThread.start()
                        continue
                if command == "abort":
                    if pcaId and pcaId in self.PCAs:
                        self.abortFunction(pcaId)
                        self.commandSocket.send(codes.ok)
                    else:
                        self.commandSocket.send(codes.error)
                    continue
                if command == "start":
                    if pcaId and pcaId in self.PCAs:
                        self.commandSocket.send(codes.ok)
                        self.transition(pcaId,"start",tag)
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
                self.transition(pcaId,"abort")
                self.isPCAinTransition[pcaId] = False
                self.sendUpdate(pcaId,"transition aborted")
                return
            self.transition(pcaId,"success",tag)
            self.isPCAinTransition[pcaId] = False
            self.sendUpdate(pcaId)
        else:
            self.transition(pcaId,"error")
            self.sendUpdate(pcaId,"transition failed")
            self.isPCAinTransition[pcaId] = False

    def abortFunction(self,pcaId):
        #terminate Transition if active
        if self.isPCAinTransition[pcaId]:
            self.abort = True
            self.scriptProcess.terminate()
        else:
            self.transition(pcaId,"abort")
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
            print(data)
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
        super().__init__("FLES")


    def waitForCommands(self):
        while True:
            try:
                command = self.commandSocket.recv_multipart()
                pcaId = None
                tag = None
                if len(command) > 1:
                    pcaId = command[1].decode()
                if len(command) > 2:
                    tag = command[2].decode()
                command = command[0]
                if command == codes.ping:
                    self.commandSocket.send(codes.ok)
                    continue
                if command == codes.pcaAsksForDetectorStatus:
                    if pcaId and pcaId in self.PCAs:
                        if pcaId in self.pcaConfigTag:
                            self.commandSocket.send_multipart([self.StateMachineForPca[pcaId].currentState.encode(),self.pcaConfigTag[pcaId].encode()])
                        else:
                            self.commandSocket.send_multipart([self.StateMachineForPca[pcaId].currentState.encode()])
                    continue
                command=command.decode()
                print(command)
                if command == "configure":
                    if self.StateMachineForPca[pcaId].currentState not in {"Active","Unconfigured"}:
                        self.commandSocket.send(codes.busy)
                        continue
                    elif not self.StateMachineForPca[pcaId].checkIfPossible("configure") or not tag or not pcaId:
                        self.commandSocket.send(codes.error)
                        print("error")
                        continue
                    else:
                        self.commandSocket.send(codes.ok)
                        self.transition(pcaId,"configure",tag)
                        self.isPCAinTransition[pcaId] = True
                        self.sendUpdate(pcaId)
                        workThread = threading.Thread(name="worker", target=self.configure, args=(pcaId,tag))
                        workThread.start()
                        continue
                if command == "abort":
                    if pcaId and pcaId in self.PCAs:
                        self.abortFunction(pcaId)
                        self.commandSocket.send(codes.ok)
                    else:
                        self.commandSocket.send(codes.error)
                    continue
                if command == "start":
                    if pcaId and pcaId in self.PCAs:
                        self.commandSocket.send(codes.ok)
                        self.transition(pcaId,"start",tag)
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
                self.transition(pcaId,"abort")
                self.isPCAinTransition[pcaId] = False
                self.sendUpdate(pcaId,"transition aborted")
                return
            self.transition(pcaId,"success",tag)
            self.isPCAinTransition[pcaId] = False
            self.sendUpdate(pcaId)
        else:
            self.transition(pcaId,"error")
            self.sendUpdate(pcaId,"transition failed")
            self.isPCAinTransition[pcaId] = False

    def abortFunction(self,pcaId):
        #terminate Transition if active
        if self.isPCAinTransition[pcaId]:
            self.abort = True
            self.scriptProcess.terminate()
        else:
            self.transition(pcaId,"abort")
            self.sendUpdate(pcaId)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("please enter a SystemType (TFC,DCS,QA,FLES,all)")
        sys.exit(1)
    if "TFC" in sys.argv:
        test = TFCClient()
        x = input()
    elif "DCS" in sys.argv:
        test = DCSClient()
        while True:
            try:
                x = input()
                message = x.split()
                test.waitForDCSMessages(message)
            except KeyboardInterrupt:
                test.terminate()
                break
            except EOFError:
                time.sleep(500000)
                continue
    elif "QA" in sys.argv:
        test = QAClient()
        while True:
            try:
                x = input()
                message = x.split()
                test.waitForQAMessages(message)
            except KeyboardInterrupt:
                test.terminate()
                break
            except EOFError:
                time.sleep(500000)
                continue
    elif "FLES" in sys.argv:
        test = FLESClient()
        x = input()
    elif "all" in sys.argv:
        TFCClient()
        DCSClient()
        QAClient()
        FLESClient()
        x = input()
    else:
        print("type unknown")
        sys.exit(1)
