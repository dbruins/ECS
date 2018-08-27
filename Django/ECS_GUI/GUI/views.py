from django.shortcuts import get_object_or_404,render,redirect
from guardian.shortcuts import assign_perm, get_users_with_perms, remove_perm
from django.http import HttpResponse, HttpResponseRedirect, JsonResponse
from GUI.models import Question, Choice, pcaModel
from django.template import loader
from django.urls import reverse
from django.conf import settings
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
import json
from django.contrib.auth.decorators import login_required, permission_required
from django.contrib.auth.models import Permission
from django.apps import apps
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

        #logsubscription
        self.socketSubLog = self.context.socket(zmq.SUB)
        self.socketSubLog.connect("tcp://%s:%s" % (settings.ECS_ADDRESS,settings.ECS_LOG_PORT))
        self.socketSubLog.setsockopt(zmq.SUBSCRIBE, b'')

        t = threading.Thread(name="logUpdaterECS", target=self.waitForECSLogs)
        t.start()

        #get all partitions
        while self.pcaCollection == None:
            ret = self.request(settings.ECS_ADDRESS,settings.ECS_REQUEST_PORT,[ECSCodes.getAllPCAs])
            if ret == ECSCodes.timeout:
                self.log("timeout getting PCA List")
                print("timeout getting PCA List")
            else:
                pJSON = ret.decode()
                pJSON = json.loads(pJSON)
                self.pcaCollection = DataObjectCollection(pJSON,partitionDataObject)

        #clear database from Previous runs
        pcaModel.objects.all().delete()

        #create Handlers for PCAs
        for p in self.pcaCollection:
            self.pcaHandlers[p.id] = PCAHandler(p)
            #add database object for storing user permissions
            pcaModel.objects.create(id=p.id)


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

    def waitForECSLogs(self):
        """wait for new log messages from ecs"""
        while True:
            m = self.socketSubLog.recv().decode()
            self.log(m)


    def log(self,message):
        """spread log message through websocket(channel)"""
        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)(
            #group name
            "ecs",
            {
                #method called in consumer
                'type': 'logUpdate',
                'text': message
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
        self.stateMap = ECS_tools.MapWrapper()

        self.PCAConnection = False
        self.receive_timeout = settings.TIMEOUT
        self.pingTimeout = settings.PINGTIMEOUT
        self.pingInterval = settings.PINGINTERVAL

        self.commandSocketAddress = "tcp://%s:%s" % (self.address,self.portCommand)

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
        r = ECS_tools.getStateSnapshot(self.stateMap,partitionInfo.address,partitionInfo.portCurrentState,timeout=self.receive_timeout,pcaid=self.id)
        if r:
            self.PCAConnection = True

        t.start()
        t = threading.Thread(name="logUpdater", target=self.waitForLogUpdates)
        t.start()
        t = threading.Thread(name="heartbeat", target=self.pingHandler)
        t.start()

    def pingHandler(self):
        """send heartbeat/ping"""
        while True:
            nextPing = time.time() + self.pingInterval
            socket = self.createCommandSocket()
            socket.setsockopt(zmq.RCVTIMEO, self.pingTimeout)
            socket.send(ECSCodes.ping)
            try:
                r = socket.recv()
            except zmq.Again:
                self.handleDisconnection()
            finally:
                socket.close()

                nextPing = time.time() + self.pingInterval
            if time.time() > nextPing:
                nextPing = time.time() + self.pingInterval
            else:
                time.sleep(self.pingInterval)

    def sendCommand(self,command,arg=None):
        """send command to pca return True on Success"""
        command = [command]
        if arg:
            command.append(arg.encode())

        commandSocket = self.createCommandSocket()
        commandSocket.send_multipart(command)
        try:
            r = commandSocket.recv()
        except zmq.Again:
            self.handleDisconnection()
            return False
        finally:
            commandSocket.close()
        if r != ECSCodes.ok:
            self.log("received error for sending command: " + command)
            return False
        return True

    def createCommandSocket(self):
        """init or reset the command Socket"""
        socket = self.context.socket(zmq.REQ)
        socket.connect(self.commandSocketAddress)
        socket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        socket.setsockopt(zmq.LINGER,0)
        return socket

    def handleDisconnection(self):
        if self.PCAConnection:
            self.log("PCA %s Connection Lost" % self.id)
        #reset commandSocket
        self.createCommandSocket()
        self.PCAConnection = False
        r = ECS_tools.getStateSnapshot(self.stateMap,self.address,self.portCurrentState,timeout=self.receive_timeout,pcaid=self.id)
        if r:
            self.log("PCA %s connected" % self.id)
            self.PCAConnection = True

    def setActive(self,detectorId):
        socket = self.context.socket(zmq.REQ)
        socket.connect(self.commandSocketAddress)
        socket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        socket.setsockopt(zmq.LINGER,0)

        socket.send_multipart([ECSCodes.setActive,detectorId.encode()])

        socket.close()

    def waitForUpdates(self):
        while True:
            m = self.socketSubscription.recv_multipart()
            if len(m) != 3:
                print (m)
            else:
                id,sequence,state = m


            id = id.decode()
            sequence = ECS_tools.intFromBytes(sequence)

            if state == ECSCodes.reset:
                self.stateMap.reset()
                #reset code for Web Browser
                state = "reset"
            elif state == ECSCodes.removed:
                del self.stateMap[id]
                #remove code for Web Browser
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
            self.sendUpdateToWebsockets("update",jsonWebUpdate)

    def log(self,message):
        """spread log message through websocket(channel)"""
        self.sendUpdateToWebsockets("logUpdate",message)

    def sendUpdateToWebsockets(self,type,message):
        channel_layer = get_channel_layer()
        #pca page
        async_to_sync(channel_layer.group_send)(
            #the group name
            self.id,
            {
                #calls method update in the consumer which is registered to channel layer
                'type': type,
                #argument(s) with which update is called
                'text': message
            }
        )
        #ecs page
        async_to_sync(channel_layer.group_send)(
            #the group name
            "ecs",
            {
                #calls method update in the consumer which is registered to channel layer
                'type': type,
                #argument(s) with which update is called
                'text': message,
                #ecs page needs to know where the update came from
                'origin': self.id
            }
        )
    def waitForLogUpdates(self):
        """wait for new log messages from PCA"""
        while True:
            m = self.socketSubLog.recv().decode()
            self.log(m)

ecs = ECSHandler()


# from django.contrib.auth import user_logged_in, user_logged_out
# from django.dispatch import receiver
# gui = apps.get_app_config('GUI')
# print(gui.test)
#
# @receiver(user_logged_in)
# def on_user_logged_in(sender, request, **kwargs):
#     print(request.user)

def checkIfHasControl(user,pcaId):
    pcaObject = pcaModel.objects.filter(id=pcaId).get()
    if not user.has_perm("has_control",pcaObject):
        return False
    return True

#views
@login_required
def index(request):
    #create Data Map for template
    ecsMap = {}
    for pca in ecs.pcaHandlers.items():
        ecsMap[pca[0]] = pca[1].stateMap.map
    return render(request, "GUI/ECS.html",{"ecsMap" : ecsMap, })

@login_required
def input_create_pca(request):
    unmappedDetectors = ecs.getUnmappedDetectors()
    return render(request, 'GUI/ECS_Create_Partition.html',{"unmappedDetectors" : unmappedDetectors})

@login_required
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
        return HttpResponseRedirect('/',{"pcaList" : ecs.pcaHandlers.items()})
    unmappedDetectors = ecs.getUnmappedDetectors()
    return render(request, 'GUI/ECS_Create_Partition.html', {"error" : True, "post":request.POST, "unmappedDetectors" : unmappedDetectors})

@login_required
def input_edit_pca(request):
    id = request.POST["id"]
    partition = ecs.getPartition(id)
    unmappedDetectors = ecs.getUnmappedDetectors()
    return render(request, 'GUI/ECS_Edit_Partition.html', {"pca":partition, "unmappedDetectors" : unmappedDetectors})

@login_required
def edit_pca(request):
    pass

@login_required
def input_create_detector(request):
    return render(request, 'GUI/ECS_Create_Detector.html',{"pcaList" : ecs.pcaHandlers.items()})

@login_required
def create_detector(request):
    values = {
            "id" : request.POST["id"],
            "address": request.POST["address"],
            "type" : request.POST["type"],
            "portTransition" : int(request.POST["portTransition"]),
            "portCommand" : int(request.POST["portCommand"]),
            }
    if request.POST["partition"] == "None":
        partition = None
    else:
        partition = request.POST["partition"]
    obj = detectorDataObject(values)
    if ecs.createDetector(obj):
        if partition:
            ecs.mapDetectors(partition,[obj.id])
        return HttpResponseRedirect('/',{"pcaList" : ecs.pcaHandlers.items()})
    return render(request, '/ECS_Create_Detector.html', {"error" : True, "post":request.POST, "pcaList" : ecs.pcaHandlers.items()})

@login_required
def pca(request,pcaId):
    pca = ecs.getPCAHandler(pcaId)
    pcaObject = pcaModel.objects.filter(id=pcaId).get()

    #check if another user has control over pca
    userInControl = None
    usersWithPermission = get_users_with_perms(pcaObject, attach_perms = True)
    for user, perms in usersWithPermission.items():
        if "has_control" in perms:
            userInControl = user
            break
    if userInControl == request.user:
        userInControl = "You"
    print(userInControl)
    return render(request, "GUI/monitor.html",{'stateMap': pca.stateMap.map, "pcaId" : pcaId, "pcaObject" : pcaObject, "userInControl":userInControl})

@login_required
def setActive(request,pcaId):
    if not checkIfHasControl(request.user,pcaId):
        return HttpResponse(status=403)
    detectorId = request.POST["detectorId"]
    pca = ecs.getPCAHandler(pcaId)
    pca.sendCommand(ECSCodes.setActive,detectorId)
    return HttpResponse(status=200)

@login_required
def setInactive(request,pcaId):
    if not checkIfHasControl(request.user,pcaId):
        return HttpResponse(status=403)
    detectorId = request.POST["detectorId"]
    pca = ecs.getPCAHandler(pcaId)
    pca.sendCommand(ECSCodes.setInactive,detectorId)
    return HttpResponse(status=200)

@login_required
def ready(request,pcaId):
    if not checkIfHasControl(request.user,pcaId):
        return HttpResponse(status=403)
    print ("sending ready")
    pca = ecs.getPCAHandler(pcaId)
    pca.sendCommand(ECSCodes.getReady)
    return HttpResponse(status=200)

@login_required
def start(request,pcaId):
    if not checkIfHasControl(request.user,pcaId):
        return HttpResponse(status=403)
    print ("sending start")
    pca = ecs.getPCAHandler(pcaId)
    pca.sendCommand(ECSCodes.start)
    return HttpResponse(status=200)

@login_required
def shutdown(request,pcaId):
    if not checkIfHasControl(request.user,pcaId):
        return HttpResponse(status=403)
    print ("sending shutdown")
    pca = ecs.getPCAHandler(pcaId)
    pca.sendCommand(ECSCodes.shutdown)
    return HttpResponse(status=200)

@login_required
def stop(request,pcaId):
    if not checkIfHasControl(request.user,pcaId):
        return HttpResponse(status=403)
    print ("sending stop")
    pca = ecs.getPCAHandler(pcaId)
    pca.sendCommand(ECSCodes.stop)
    return HttpResponse(status=200)

@login_required
def input_MoveDetectors(request):
    return render(request, 'GUI/ECS_moveDetectors.html',{"partitions" : ecs.pcaHandlers.items()})

@login_required
def moveDetectors(request):
    toPcaId = request.POST['toPartition']
    detectors = request.POST.getlist("selectedDetectors")
    for dId in detectors:
        print(dId,toPcaId)
        if not ecs.moveDetector(dId,toPcaId):
            return render(request, 'GUI/ECS_moveDetectors.html',{"partitions" : ecs.pcaHandlers.items(), "error":True})
    return HttpResponseRedirect('/',{"pcaList" : ecs.pcaHandlers.items()})

@login_required
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

@permission_required('GUI.can_take_control')
@login_required
def takeControl(request,pcaId):
    pca = ecs.getPCAHandler(pcaId)
    pcaObject = pcaModel.objects.filter(id=pcaId).get()

    #check if other user has control for pca
    usersWithPermission = get_users_with_perms(pcaObject, attach_perms = True)
    for user, perms in usersWithPermission.items():
        if "has_control" in perms:
            return HttpResponseRedirect("/pca/"+pcaId,{'stateMap': pca.stateMap.map, "pcaId" : pcaId, "pcaObject" : pcaObject})
    assign_perm('has_control', request.user, pcaObject)
    return HttpResponseRedirect("/pca/"+pcaId,{'stateMap': pca.stateMap.map, "pcaId" : pcaId, "pcaObject" : pcaObject})

@permission_required('GUI.can_take_control')
@login_required
def giveUpControl(request,pcaId):
    pca = ecs.getPCAHandler(pcaId)
    pcaObject = pcaModel.objects.filter(id=pcaId).get()
    remove_perm('has_control', request.user, pcaObject)

    return HttpResponseRedirect("/pca/"+pcaId,{'stateMap': pca.stateMap.map, "pcaId" : pcaId, "pcaObject" : pcaObject})
