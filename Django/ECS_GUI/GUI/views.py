from django.shortcuts import get_object_or_404,render,redirect
from django.http import HttpResponse, HttpResponseRedirect, JsonResponse
from GUI.models import Question, Choice
from django.template import loader
from django.urls import reverse
from django.conf import settings
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
import json

import zmq
import threading
import struct
from multiprocessing import Queue
import time
import configparser

import sys
#ECS Codes and DataObjects should be in the same Path later
projectPath = settings.PCACODESPATH
sys.path.append(projectPath)
import ECSCodes
import ECS_tools
from DataObjects import DataObjectCollection, detectorDataObject, partitionDataObject

class ECSHandler:

    def __init__(self):
        self.context = zmq.Context()
        self.pcaCollection = None
        self.pcaHandlers = {}

        #get all partitions
        while self.pcaCollection == None:
            ret = self.request(settings.ECS_ADDRESS,settings.ECS_REQUEST_PORT,[ECSCodes.getAllPCAs])
            if ret == ECSCodes.timeout:
                self.log("timeout getting PCA List")
            else:
                pJSON = ret.decode()
                pJSON = json.loads(pJSON)
                self.pcaCollection = DataObjectCollection(pJSON,partitionDataObject)

        #create Handlers for PCAs
        for p in self.pcaCollection:
            self.pcaHandlers[p.id] = PCAHandler(p)

    def request(self,address,port,message):
        """sends a request from a REQ to REP socket; sends with send_multipart so message hast to be a list; returns encoded return message or timeout code when recv times out"""
        requestSocket = self.context.socket(zmq.REQ)
        requestSocket.connect("tcp://%s:%s" % (address,port))
        requestSocket.setsockopt(zmq.RCVTIMEO, int(settings.TIMEOUT))
        requestSocket.setsockopt(zmq.LINGER,0)

        requestSocket.send_multipart(message)
        try:
            ret = requestSocket.recv()
            print(ret)
            requestSocket.close()
            return ret
        except zmq.Again:
            requestSocket.close()
            return ECSCodes.timeout

    def getPCAHandler(self,id):
        if id in self.pcaHandlers:
            return self.pcaHandlers[id]
        else:
            return None

    def getPartition(self,id):
        ret = self.request(settings.ECS_ADDRESS,settings.ECS_REQUEST_PORT,[ECSCodes.getPartitionForId, id.encode()])
        if ret == ECSCodes.timeout:
            self.log("timeout getting Partition")
            return False
        partition = partitionDataObject(json.loads(ret.decode()))
        return partition

    def createPartition(self,partitionObject,detectorsIds):
        message = {}
        message["partition"] = partitionObject.asJsonString()
        message["detectors"] = detectorsIds
        ret = self.request(settings.ECS_ADDRESS,settings.ECS_REQUEST_PORT,[ECSCodes.createPartition, json.dumps(message).encode()])
        if ret == ECSCodes.ok:
            #connect to new PCA
            self.pcaCollection.add(partitionObject)
            self.pcaHandlers[partitionObject.id] = PCAHandler(partitionObject)
            self.log("partition %s created" % partitionObject.id)
            return True
        if ret == ECSCodes.timeout:
            self.log("timeout creating partition")
        if ret == ECS.errorCreatingPartition:
            self.log("error creating Partition")
        if ret == ECSCodes.errorMapping:
            self.log("error mapping detectors to Partition")
        return False

    def createDetector(self,detectorObject):
        ret = self.request(settings.ECS_ADDRESS,settings.ECS_REQUEST_PORT,[ECSCodes.createDetector, detectorObject.asJsonString().encode()])
        if ret == ECSCodes.ok:
            self.log("Detector %s created" % detectorObject.id)
            return True
        if ret == ECSCodes.timeout:
            self.log("timeout creating Detector")
        else:
            self.log("error creating Detector")
        return False


    def getUnmappedDetectors(self):
        """request currently unmapped Detector List from ECS databse"""
        ret = self.request(settings.ECS_ADDRESS,settings.ECS_REQUEST_PORT,[ECSCodes.getUnmappedDetectors])
        if ret == ECSCodes.timeout:
            self.log("timeout getting Unmapped Detectors")
            return False
        else:
            dJSON = ret.decode()
            dJSON = json.loads(dJSON)
            return DataObjectCollection(dJSON,detectorDataObject)


    def mapDetectors(self,pcaid,detectorList):
        jsonArg = {}
        for d in detectorList:
            jsonArg[d] = pcaid
        ret = self.request(settings.ECS_ADDRESS,settings.ECS_REQUEST_PORT,[ECSCodes.mapDetectorsToPCA, json.dumps(jsonArg).encode()])
        if ret == ECSCodes.ok:
            self.log("Detectors mapped")
            return True
        if ret == ECSCodes.timeout:
            self.log("timeout mapping Detectors")
        else:
            self.log("error mapping Detectors")
        return False

    def getDetectorListForPartition(self,pcaid):
        """get Detector List from Database for a pcaId"""
        ret = self.request(settings.ECS_ADDRESS,settings.ECS_REQUEST_PORT,[ECSCodes.pcaAsksForDetectorList, pcaid.encode()])
        if ret == ECSCodes.error:
            self.log("error getting Detectors for pca %s" % pcaid)
            return ECSCodes.error
        if ret == ECSCodes.timeout:
            self.log("timeout getting Detectors for pca %s" % pcaid)
            return ECSCodes.error
        detList = DataObjectCollection(json.loads(ret),detectorDataObject)
        return detList

    def moveDetector(self,detectorId,toPCAId):
        """maps Detector to Partition (ECS knows where the Detector currently is)"""
        arg={}
        arg["detectorId"] = detectorId
        arg["partitionId"] = toPCAId
        ret = self.request(settings.ECS_ADDRESS,settings.ECS_REQUEST_PORT,[ECSCodes.detectorChangePartition, json.dumps(arg).encode()])
        if ret == ECSCodes.error:
            self.log("error moving Detector to pca %s" % toPCAId)
            return False
        if ret == ECSCodes.timeout:
            self.log("timeout moving Detector to pca %s" % toPCAId)
            return False
        return True

    def log(self,message):
        """spread log message through websocket(channel)"""
        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)(
            "update",
            {
                'type': 'logUpdate',
                'logText': message
            }
        )


class PCAHandler:
    def __init__(self,partitionInfo):
        self.id = partitionInfo.id
        self.address = partitionInfo.address
        self.portLog = partitionInfo.portLog
        self.portCommand = partitionInfo.portCommand
        self.portPublish = partitionInfo.portPublish
        self.portCurrentState = partitionInfo.portCurrentState

        self.context = zmq.Context()
        self.commandSocketQueue = Queue()
        self.stateMap = ECS_tools.MapWrapper()

        self.PCAConnection = False
        self.receive_timeout = settings.TIMEOUT
        self.pingTimeout = settings.PINGTIMEOUT
        self.pingIntervall = settings.PINGINTERVALL

        self.socketGetCurrentStateTable = None
        self.commandSocket = None
        self.commandSocketAddress = "tcp://%s:%s" % (self.address,self.portCommand)
        self.createCommandSocket()

        #state Change subscription
        self.socketSubscription = self.context.socket(zmq.SUB)
        self.socketSubscription.connect("tcp://%s:%s" % (self.address, self.portPublish))
        #subscribe to everything
        self.socketSubscription.setsockopt(zmq.SUBSCRIBE, b'')

        #logsubscription
        self.socketSubLog = self.context.socket(zmq.SUB)
        self.socketSubLog.connect("tcp://%s:%s" % (self.address,self.portLog))
        self.socketSubLog.setsockopt(zmq.SUBSCRIBE, b'')

        t = threading.Thread(name="updater", target=self.waitForUpdates)
        ECS_tools.getStateSnapshot(self.stateMap,partitionInfo.address,partitionInfo.portCurrentState,timeout=self.receive_timeout,pcaid=self.id)
        t.start()
        t = threading.Thread(name="logUpdater", target=self.waitForLogUpdates)
        t.start()
        t = threading.Thread(name="heartbeat", target=self.commandSocketHandler)
        t.start()

    def putCommand(self,command,arg=None):
        """put a command with an optinal argument in the command Queue"""
        command = [command]
        if arg:
            command.append(arg.encode())
        self.commandSocketQueue.put(command)


    def commandSocketHandler(self):
        """send heartbeat/ping and commands on command socket"""
        nextPing = time.time() + self.pingIntervall
        while True:
            if not self.commandSocketQueue.empty():
                m = self.commandSocketQueue.get()
                if m == ECSCodes.ping:
                    self.commandSocket.setsockopt(zmq.RCVTIMEO, self.pingTimeout)
                else:
                    self.commandSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
                r = self.sendCommand(m)
                if not r:
                    #try to resend later ?
                    #self.putCommand(m)
                    continue
                if m != ECSCodes.ping:
                    #we've just send a message we don't need a ping
                    nextPing = time.time() + self.pingIntervall
            if time.time() > nextPing:
                self.putCommand(ECSCodes.ping)
                nextPing = time.time() + self.pingIntervall

    def sendCommand(self,command):
        """send command to pca return True on Success"""
        self.commandSocket.send_multipart(command)
        try:
            r = self.commandSocket.recv()
        except zmq.Again:
            self.handleDisconnection()
            return False
        if r != ECSCodes.ok:
            self.log("received error for sending command: " + command)
            return False
        return True

    def createCommandSocket(self):
        """init or reset the command Socket"""
        if(self.commandSocket):
            #reset
            self.commandSocket.close()
        self.commandSocket = self.context.socket(zmq.REQ)
        self.commandSocket.connect(self.commandSocketAddress)
        self.commandSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        self.commandSocket.setsockopt(zmq.LINGER,0)

    def handleDisconnection(self):
        self.log("PCA %s Connection Lost" % self.id)
        #reset commandSocket
        self.createCommandSocket()
        self.PCAConnection = False
        ECS_tools.getStateSnapshot(self.stateMap,self.address,self.portCurrentState,timeout=self.receive_timeout,pcaid=self.id)

    def setActive(self,detectorId):
        socket = self.context.socket(zmq.REQ)
        socket.connect(self.commandSocketAddress)
        socket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        socket.setsockopt(zmq.LINGER,0)

        socket.send_multipart([ECSCodes.setActive,detectorId.encode()])

        socket.close()

    def receive_status(self,socket):
        try:
            id, sequence, state = socket.recv_multipart()
        except zmq.Again:
            return None
        except:
            print ("error while receiving")
            return
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

    def waitForUpdates(self):
        while True:
            m = self.socketSubscription.recv_multipart()
            if len(m) != 3:
                print (m)
            else:
                id,sequence,state = m


            id = id.decode()
            sequence = struct.unpack("!i",sequence)[0]

            if state == ECSCodes.reset:
                self.stateMap.reset()
                state = "reset"
            elif state == ECSCodes.removed:
                del self.stateMap[id]
                state = "remove"
            else:
                state = state.decode()
                print("received update",id, sequence, state)
                self.stateMap[id] = (sequence, state)

            #send update to WebUI(s)
            jsonWebUpdate = {"id" : id,
                             "state" : state,
                            }
            jsonWebUpdate = json.dumps(jsonWebUpdate)
            channel_layer = get_channel_layer()
            async_to_sync(channel_layer.group_send)(
                #the group name
                "update",
                {
                    #calls method update in the consumer which is registered to channel layer
                    'type': 'update',
                    #argument(s) with which update is called
                    'text': jsonWebUpdate
                }
            )

    def log(self,message):
        """spread log message through websocket(channel)"""
        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)(
            #the group name
            "update",
            {
                #calls method update in the consumer which is registered to channel layer
                'type': 'logUpdate',
                #argument(s) with which update is called
                'logText': message
            }
        )

    def waitForLogUpdates(self):
        """wait for new log messages from PCA"""
        while True:
            m = self.socketSubLog.recv().decode()
            self.log(m)

ecs = ECSHandler()
#views
def index(request):
    #create Data Map for template
    ecsMap = {}
    for pca in ecs.pcaHandlers.items():
        ecsMap[pca[0]] = pca[1].stateMap.map
    return render(request, "GUI/ECS.html",{"ecsMap" : ecsMap, })

def input_create_pca(request):
    unmappedDetectors = ecs.getUnmappedDetectors()
    return render(request, 'GUI/ECS_Create_Partition.html',{"unmappedDetectors" : unmappedDetectors})

def create_pca(request):
    values = {
            "id" : request.POST["id"],
            "address": request.POST["address"],
            "portPublish" : int(request.POST["portPublish"]),
            "portLog" : int(request.POST["portLog"]),
            "portUpdates" : int(request.POST["portUpdates"]),
            "portCurrentState" : int(request.POST["portCurrentState"]),
            "portSingleRequest" : int(request.POST["portSingleRequest"]),
            "portCommand" : int(request.POST["portCommand"]),
            }
    selectedDetectors = request.POST.getlist("unmappedDetectors")
    obj = partitionDataObject(values)
    if ecs.createPartition(obj,selectedDetectors):
        return HttpResponseRedirect('/GUI/',{"pcaList" : ecs.pcaHandlers.items()})
    unmappedDetectors = ecs.getUnmappedDetectors()
    return render(request, 'GUI/ECS_Create_Partition.html', {"error" : True, "post":request.POST, "unmappedDetectors" : unmappedDetectors})

def input_edit_pca(request):
    id = request.POST["id"]
    partition = ecs.getPartition(id)
    unmappedDetectors = ecs.getUnmappedDetectors()
    return render(request, 'GUI/ECS_Edit_Partition.html', {"pca":partition, "unmappedDetectors" : unmappedDetectors})
    
def edit_pca(request):
    pass

def input_create_detector(request):
    return render(request, 'GUI/ECS_Create_Detector.html',{"pcaList" : ecs.pcaHandlers.items()})

def create_detector(request):
    values = {
            "id" : request.POST["id"],
            "address": request.POST["address"],
            "type" : request.POST["type"],
            "port" : int(request.POST["port"]),
            "pingPort" : int(request.POST["pingPort"]),
            }
    if request.POST["partition"] == "None":
        partition = None
    else:
        partition = request.POST["partition"]
    obj = detectorDataObject(values)
    if ecs.createDetector(obj):
        if partition:
            ecs.mapDetectors(partition,[obj.id])
        return HttpResponseRedirect('/GUI/',{"pcaList" : ecs.pcaHandlers.items()})
        #return HttpResponseRedirect(reverse(index))
        #return render(request, "GUI/ECS.html",{"pcaList" : ecs.pcaHandlers.items()})
    return render(request, 'GUI/ECS_Create_Detector.html', {"error" : True, "post":request.POST, "pcaList" : ecs.pcaHandlers.items()})

def pca(request,pcaId):
    pca = ecs.getPCAHandler(pcaId)
    print (pca.stateMap)
    return render(request, "GUI/monitor.html",{'stateMap': pca.stateMap.map, "pcaId" : pcaId})

def setActive(request,pcaId):
    detectorId = request.POST["detectorId"]
    pca = ecs.getPCAHandler(pcaId)
    pca.putCommand(ECSCodes.setActive,detectorId)
    return HttpResponse(status=200)

def setInactive(request,pcaId):
    detectorId = request.POST["detectorId"]
    pca = ecs.getPCAHandler(pcaId)
    pca.putCommand(ECSCodes.setInactive,detectorId)
    return HttpResponse(status=200)

def ready(request,pcaId):
    print ("sending ready")
    pca = ecs.getPCAHandler(pcaId)
    pca.putCommand(ECSCodes.getReady)
    return HttpResponse(status=200)

def start(request,pcaId):
    print ("sending start")
    pca = ecs.getPCAHandler(pcaId)
    pca.putCommand(ECSCodes.start)
    return HttpResponse(status=200)

def shutdown(request,pcaId):
    print ("sending shutdown")
    pca = ecs.getPCAHandler(pcaId)
    pca.putCommand(ECSCodes.shutdown)
    return HttpResponse(status=200)

def stop(request,pcaId):
    print ("sending stop")
    pca = ecs.getPCAHandler(pcaId)
    pca.putCommand(ECSCodes.stop)
    return HttpResponse(status=200)

def input_MoveDetectors(request):
    return render(request, 'GUI/ECS_moveDetectors.html',{"partitions" : ecs.pcaHandlers.items()})

def moveDetectors(request):
    toPcaId = request.POST['toPartition']
    detectors = request.POST.getlist("selectedDetectors")
    for dId in detectors:
        print(dId,toPcaId)
        if not ecs.moveDetector(dId,toPcaId):
            return render(request, 'GUI/ECS_moveDetectors.html',{"partitions" : ecs.pcaHandlers.items(), "error":True})
    return HttpResponseRedirect('/GUI/',{"pcaList" : ecs.pcaHandlers.items()})

def getDetectorListForPCA(request):
    """Ask ECS for DetectorList from Database"""
    pcaId = request.POST['pcaId']
    print(pcaId)
    detList = ecs.getDetectorListForPartition(pcaId)
    if detList == ECSCodes.error:
        return HttpResponse(status=404)
    else:
        print(detList.asDictionary())
        return JsonResponse(detList.asDictionary())
