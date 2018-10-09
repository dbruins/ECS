from django.shortcuts import get_object_or_404,render,redirect
from guardian.shortcuts import assign_perm, get_users_with_perms, remove_perm, get_user_perms
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
from django.utils import timezone
import configparser
from django import forms
from collections import deque
import sys
#ECS Codes and DataObjects should be in the same Path later
projectPath = settings.PCACODESPATH
sys.path.append(projectPath)
from ECSCodes import ECSCodes
codes = ECSCodes()
import ECS_tools
from DataObjects import DataObjectCollection, detectorDataObject, partitionDataObject, stateObject

class ECSHandler:

    def __init__(self):
        self.context = zmq.Context()
        self.pcaCollection = None
        self.pcaHandlers = {}
        self.receive_timeout = settings.TIMEOUT
        self.logQueue = deque(maxlen=settings.BUFFERED_LOG_ENTRIES)

        #logsubscription
        self.socketSubLog = self.context.socket(zmq.SUB)
        self.socketSubLog.connect("tcp://%s:%s" % (settings.ECS_ADDRESS,settings.ECS_LOG_PORT))
        self.socketSubLog.setsockopt(zmq.SUBSCRIBE, b'')

        t = threading.Thread(name="logUpdaterECS", target=self.waitForECSLogs)
        t.start()

        #get all partitions
        while self.pcaCollection == None:
            ret = self.request(settings.ECS_ADDRESS,settings.ECS_REQUEST_PORT,[codes.getAllPCAs])
            if ret == codes.timeout:
                self.log("timeout getting PCA List")
            else:
                pJSON = ret.decode()
                pJSON = json.loads(pJSON)
                self.pcaCollection = DataObjectCollection(pJSON,partitionDataObject)

        #clear database from Previous runs
        pcaModel.objects.all().delete()

        #create Handlers for PCAs
        for p in self.pcaCollection:
            self.pcaHandlers[p.id] = PCAHandler(p,self.log)
            #add database object for storing user permissions
            pcaModel.objects.create(id=p.id,permissionTimestamp=timezone.now())
        #user Permissions ecs
        pcaModel.objects.create(id="ecs",permissionTimestamp=timezone.now())

        #Unused Detector Controller
        self.unmappedStateTable = ECS_tools.MapWrapper()
        #UpdateSubscription
        self.socketSubUpdate = self.context.socket(zmq.SUB)
        self.socketSubUpdate.connect("tcp://%s:%s" % (settings.ECS_ADDRESS,settings.ECS_PUBLISHPORT))
        self.socketSubUpdate.setsockopt(zmq.SUBSCRIBE, b'')
        ECS_tools.getStateSnapshot(self.unmappedStateTable,settings.ECS_ADDRESS,settings.ECS_GET_STATETABLE_PORT,timeout=self.receive_timeout,pcaid="unmapped")
        t = threading.Thread(name="logUpdaterECS", target=self.waitForUnmappedDetectorUpdate)
        t.start()

    def request(self,address,port,message):
        """sends a request from a REQ to REP socket; sends with send_multipart so message has to be a list; returns encoded returnmessage or timeout code when recv times out"""
        requestSocket = self.context.socket(zmq.REQ)
        requestSocket.connect("tcp://%s:%s" % (address,port))
        requestSocket.setsockopt(zmq.RCVTIMEO, int(settings.TIMEOUT))
        requestSocket.setsockopt(zmq.LINGER,0)

        requestSocket.send_multipart(message)
        try:
            ret = requestSocket.recv()
            requestSocket.close()
            return ret
        except zmq.Again:
            requestSocket.close()
            return codes.timeout
        finally:
            requestSocket.close()

    def getPCAHandler(self,id):
        if id in self.pcaHandlers:
            return self.pcaHandlers[id]
        else:
            return None

    def getPartition(self,id):
        ret = self.request(settings.ECS_ADDRESS,settings.ECS_REQUEST_PORT,[codes.getPartitionForId, id.encode()])
        if ret == codes.timeout:
            self.log("timeout getting Partition")
            return False
        if ret == codes.timeout:
            self.log("partition id is unknown")
            return False
        partition = partitionDataObject(json.loads(ret.decode()))
        return partition

    def deletePartition(self,partitionId,forceDelete=False):
        arg = {}
        arg["partitionId"] = partitionId
        if forceDelete:
            arg["forceDelete"] = True
        ret = self.request(settings.ECS_ADDRESS,settings.ECS_REQUEST_PORT,[codes.deletePartition, json.dumps(arg).encode()])
        if ret == codes.ok:
            #connect to new PCA
            handler = self.pcaHandlers[partitionId]
            #terminate Handlers
            handler.terminatePCAHandler()
            del self.pcaHandlers[partitionId]
            self.log("partition %s deleted" % partitionId)
            #delete Model for permissions
            pcaModel.objects.filter(id=partitionId).delete()
            return True
        if ret == codes.timeout:
            self.log("timeout deleting partition")
            return "timeout deleting partition"
        if ret == codes.idUnknown:
            self.log("Partition Id is unknown")
            return "Partition Id is unknown"
        if ret == codes.error:
            self.log("ECS returned error code deleting partition")
            return "ECS returned error code deleting partition"
        else:
            try:
                errorDict = json.loads(ret.decode())
                return errorDict["error"]
            except:
                self.log("ECS returned unknown error deleting partition")
                return "unknown Error"


    def createPartition(self,partitionObject):
        message = {}
        message["partition"] = partitionObject.asJsonString()
        ret = self.request(settings.ECS_ADDRESS,settings.ECS_REQUEST_PORT,[codes.createPartition, json.dumps(message).encode()])
        if ret == codes.ok:
            #connect to new PCA
            self.pcaHandlers[partitionObject.id] = PCAHandler(partitionObject,self.log)
            self.log("partition %s created" % partitionObject.id)
            #add Model for permissions
            pcaModel.objects.create(id=partitionObject.id,permissionTimestamp=timezone.now())
            return True
        if ret == codes.timeout:
            self.log("timeout creating partition")
            return "timeout creating partition"
        if ret == codes.error:
            self.log("ecs returned error code for creating Partition")
            return "ecs returned error code"
        else:
            try:
                errorDict = json.loads(ret.decode())
                self.log("error creating pca: %s" % (errorDict["error"]))
                return errorDict["error"]
            except:
                self.log("ECS returned unknown error creating partition")
                return "unknown Error"

    def createDetector(self,detectorObject):
        ret = self.request(settings.ECS_ADDRESS,settings.ECS_REQUEST_PORT,[codes.createDetector, detectorObject.asJsonString().encode()])
        if ret == codes.ok:
            self.log("Detector %s created" % detectorObject.id)
            return True
        if ret == codes.error:
            self.log("ecs returned error code for creating Detector")
            return "ecs returned error code"
        if ret == codes.timeout:
            self.log("timeout creating Detector")
            return "timeout creating Detector"
        else:
            try:
                errorDict = json.loads(ret.decode())
                return errorDict["error"]
            except:
                self.log("ECS returned unknown error creating detector")
                return "unknown Error"

    def deleteDetector(self,detectorId,forceDelete=False):
        arg = {}
        arg["detectorId"] = detectorId
        if forceDelete:
            arg["forceDelete"] = True
        ret = self.request(settings.ECS_ADDRESS,settings.ECS_REQUEST_PORT,[codes.deleteDetector, json.dumps(arg).encode()])
        if ret == codes.ok:
            self.log("Detector %s deleted" % detectorId)
            return True
        if ret == codes.timeout:
            self.log("timeout deleting Detector")
            return "timeout deleting Detector"
        if ret == codes.error:
            self.log("ECS returned error code deleting Detector")
            return "ECS returned error code deleting Detector"
        if ret == codes.connectionProblemDetector:
            self.log("Detector %s could not be reached" % detectorId)
            return "Detector %s could not be reached" % detectorId
        else:
            try:
                errorDict = json.loads(ret.decode())
                return errorDict["error"]
            except:
                self.log("ECS returned unknown error deleting Detector")
                return "unknown Error"


    def getUnmappedDetectors(self):
        """request currently unmapped Detector List from ECS databse"""
        ret = self.request(settings.ECS_ADDRESS,settings.ECS_REQUEST_PORT,[codes.getUnmappedDetectors])
        if ret == codes.timeout:
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
        ret = self.request(settings.ECS_ADDRESS,settings.ECS_REQUEST_PORT,[codes.mapDetectorsToPCA, json.dumps(jsonArg).encode()])
        if ret == codes.ok:
            self.log("Detectors mapped")
            return True
        if ret == codes.timeout:
            self.log("timeout mapping Detectors")
        else:
            self.log("error mapping Detectors")
        return False

    def getDetectorListForPartition(self,pcaid):
        """get Detector List from Database for a pcaId"""
        ret = self.request(settings.ECS_ADDRESS,settings.ECS_REQUEST_PORT,[codes.pcaAsksForDetectorList, pcaid.encode()])
        if ret == codes.error:
            self.log("error getting Detectors for pca %s" % pcaid)
            return codes.error
        if ret == codes.timeout:
            self.log("timeout getting Detectors for pca %s" % pcaid)
            return codes.error
        detList = DataObjectCollection(json.loads(ret),detectorDataObject)
        return detList

    def moveDetector(self,detectorId,toPCAId,forceMove=False):
        """maps Detector to Partition (ECS knows where the Detector currently is)"""
        arg={}
        arg["detectorId"] = detectorId
        arg["partitionId"] = toPCAId
        if forceMove:
            arg["forceMove"] = True
        ret = self.request(settings.ECS_ADDRESS,settings.ECS_REQUEST_PORT,[codes.detectorChangePartition, json.dumps(arg).encode()])
        if ret == codes.ok:
            return True
        if ret == codes.timeout:
            self.log("timeout moving Detector %s to pca %s" % (detectorId,toPCAId))
            return "request timeout"
        if ret == codes.error:
            self.log("error moving Detector %s to pca %s" % (detectorId,toPCAId))
            return "ecs returned error code"
        if ret == codes.connectionProblemOldPartition:
            self.log("error moving Detector %s to pca %s" % (detectorId,toPCAId))
            return "Old Partition is not connected"
        if ret == codes.connectionProblemNewPartition:
            self.log("error moving Detector %s to pca %s" % (detectorId,toPCAId))
            return "New Partition is not connected"
        try:
            errorDict = json.loads(ret.decode())
            self.log("error moving Detector %s to pca %s: %s" % (detectorId,toPCAId,errorDict["error"]))
            return errorDict["error"]
        except:
            self.log("ECS returned unknown error moving Detector")
            return "unknown Error"

    def waitForUnmappedDetectorUpdate(self):
        while True:
            m = self.socketSubUpdate.recv_multipart()
            if len(m) != 3:
                print ("malformed update: "+m)
                continue
            else:
                id,sequence,state = m

            id = id.decode()
            sequence = ECS_tools.intFromBytes(sequence)

            if state == codes.reset:
                self.unmappedStateTable.reset()
                #reset code for Web Browser
                state = "reset"
            elif state == codes.removed:
                del self.unmappedStateTable[id]
                #remove code for Web Browser
                state = "remove"
            else:
                state = json.loads(state.decode())
                self.unmappedStateTable[id] = (sequence, stateObject(state))

            #send update to WebUI(s)
            jsonWebUpdate = {"id" : id,
                             "state" : state,
                            }
            jsonWebUpdate = json.dumps(jsonWebUpdate)
            channel_layer = get_channel_layer()
            async_to_sync(channel_layer.group_send)(
                #the group name
                "ecs",
                {
                    #calls method update in the consumer which is registered to channel layer
                    'type': "update",
                    #argument(s) with which update is called
                    'text': jsonWebUpdate,
                    #ecs page needs to know where the update came from
                    'origin': "unmapped"
                }
            )

    def waitForECSLogs(self):
        """wait for new log messages from ecs"""
        while True:
            m = self.socketSubLog.recv().decode()
            self.log(m)


    def log(self,message,origin="ecs"):
        """spread log message through websocket(channel)"""
        channel_layer = get_channel_layer()
        message = origin+": "+message
        self.logQueue.append(message)
        async_to_sync(channel_layer.group_send)(
            #group name
            "ecs",
            {
                #method called in consumer
                'type': 'logUpdate',
                'text': message,
                'origin': origin,
            }
        )


class PCAHandler:
    def __init__(self,partitionInfo,ecsLogfunction):
        self.id = partitionInfo.id
        self.address = partitionInfo.address
        self.portLog = partitionInfo.portLog
        self.portCommand = partitionInfo.portCommand
        self.portPublish = partitionInfo.portPublish
        self.portCurrentState = partitionInfo.portCurrentState
        self.ecsLogfunction = ecsLogfunction

        self.context = zmq.Context()
        self.stateMap = ECS_tools.MapWrapper()
        self.logQueue = deque(maxlen=settings.BUFFERED_LOG_ENTRIES)

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

    def createCommandSocket(self):
        socket = self.context.socket(zmq.REQ)
        socket.connect(self.commandSocketAddress)
        socket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        socket.setsockopt(zmq.LINGER,0)
        return socket

    def pingHandler(self):
        """send heartbeat/ping"""
        socket = self.createCommandSocket()
        while True:
            nextPing = time.time() + self.pingInterval
            try:
                socket.send(codes.ping)
                r = socket.recv()
            except zmq.error.ContextTerminated:
                break
            except zmq.Again:
                self.handleDisconnection()
                #reset Socket
                socket.close()
                socket = self.createCommandSocket()
            except Exception as e:
                self.log("Exception while sending Ping: %s" % str(e))
                socket.close()
                socket = self.createCommandSocket()
            finally:
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
        if r != codes.ok:
            self.log("received error for sending command")
            return False
        return True

    def handleDisconnection(self):
        if self.PCAConnection:
            self.log("PCA %s Connection Lost" % self.id)
        self.PCAConnection = False
        r = ECS_tools.getStateSnapshot(self.stateMap,self.address,self.portCurrentState,timeout=self.receive_timeout,pcaid=self.id)
        if r:
            self.log("PCA %s connected" % self.id)
            self.PCAConnection = True

    def waitForUpdates(self):
        while True:
            try:
                m = self.socketSubscription.recv_multipart()
            except zmq.error.ContextTerminated:
                self.socketSubscription.close()
                break
            if len(m) != 3:
                print (m)
            else:
                id,sequence,state = m


            id = id.decode()
            sequence = ECS_tools.intFromBytes(sequence)

            if state == codes.reset:
                self.stateMap.reset()
                #reset code for Web Browser
                state = "reset"
            elif state == codes.removed:
                del self.stateMap[id]
                #remove code for Web Browser
                state = "remove"
            else:
                state = json.loads(state.decode())
                self.stateMap[id] = (sequence, stateObject(state))

            #send update to WebUI(s)
            jsonWebUpdate = {"id" : id,
                             "state" : state,
                             "sequenceNumber" : sequence
                            }
            jsonWebUpdate = json.dumps(jsonWebUpdate)
            self.sendUpdateToWebsockets("update",jsonWebUpdate)

    def log(self,message):
        """spread log message through websocket(channel)"""
        self.logQueue.append(message)
        self.ecsLogfunction(message,self.id)
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

        if type != "logUpdate":
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
            try:
                m = self.socketSubLog.recv().decode()
            except zmq.error.ContextTerminated:
                self.socketSubLog.close()
                break
            self.log(m)

    def terminatePCAHandler(self):
        self.context.term()
        print("%s terminated" % self.id)

ecs = ECSHandler()


gui = apps.get_app_config('GUI')
print(gui.test)
#
from django.contrib.auth import user_logged_out
from django.dispatch import receiver

@receiver(user_logged_out)
def on_user_logged_out(sender, request, **kwargs):
    #pcas
    for pca in ecs.pcaHandlers.items():
        pcaObject = pcaModel.objects.filter(id=pca[0]).get()
        if request.user.has_perm("has_control",pcaObject):
         remove_perm('has_control', request.user, pcaObject)
    #ecs
    pcaObject = pcaModel.objects.filter(id="ecs").get()
    if request.user.has_perm("has_control",pcaObject):
        remove_perm('has_control', request.user, pcaObject)

def permission_timeout():
    """remove permission has control if user has been inactive for a while"""
    while True:
        #seconds
        time.sleep(settings.PERMISSION_TIMEOUT)
        allPCAs = pcaModel.objects.all()
        for pcaObject in allPCAs:
            usersWithPermission = get_users_with_perms(pcaObject, attach_perms = True)
            for user, perms in usersWithPermission.items():
                if "has_control" in perms:
                    timedelta = timezone.now() - pcaObject.permissionTimestamp
                    if timedelta.total_seconds() > settings.PERMISSION_TIMEOUT:
                        print(str(timedelta.total_seconds()) +" timeout user: "+str(user) )
                        remove_perm('has_control', user, pcaObject)
                        channel_layer = get_channel_layer()
                        #inform over websocket
                        async_to_sync(channel_layer.group_send)(
                            #the group name
                            str(user),
                            {
                                #calls method update in the consumer which is registered to channel layer
                                'type': "permissionTimeout",
                            }
                        )
t = threading.Thread(name="permission_timeout", target=permission_timeout)
t.start()

from django.views.generic import TemplateView,View
from django.contrib.auth.mixins import LoginRequiredMixin
from django.views.generic.edit import FormView
from guardian.mixins import PermissionRequiredMixin
class resetUserTimeoutMixin(object):
    """resets the timeout if a user makes a request"""
    def dispatch(self, request, *args, **kwargs):
        allPCAs = pcaModel.objects.all()
        for pcaObject in allPCAs:
            perm = get_user_perms(request.user,pcaObject)
            if 'has_control' in perm:
                pcaObject.permissionTimestamp = timezone.now()
                pcaObject.save()
        return super().dispatch(request, *args, **kwargs)

class ecsMixin(LoginRequiredMixin,resetUserTimeoutMixin):
    """combination of Login required and reset timeout """
    pass

class pcaForm(forms.Form):
    id = forms.CharField(label="id")
    address = forms.CharField(label="address")
    portPublish = forms.IntegerField(min_value= 1,label="Publish Port")
    portLog = forms.IntegerField(min_value= 1,label="Port Logmessages")
    portUpdates = forms.IntegerField(min_value= 1,label="Port Updates")
    portCurrentState = forms.IntegerField(min_value= 1,label="Port Current State")
    portSingleRequest = forms.IntegerField(min_value= 1)
    portCommand = forms.IntegerField(min_value= 1,label="Port Command")

class detectorForm(forms.Form):
    id = forms.CharField(label="id")
    address = forms.CharField(label="address")
    type = forms.CharField(label="type")
    portTransition = forms.IntegerField(min_value= 1,label="Port Transition")
    portCommand = forms.IntegerField(min_value= 1,label="Port Command")


#views
class index(ecsMixin,TemplateView):
    template_name = "GUI/index.html"
    ecsMap = {}
    pcaObject = pcaModel.objects.filter(id="ecs").get()
    userInControl = None
    def dispatch(self, request, *args, **kwargs):
        self.ecsMap = {}
        for pca in ecs.pcaHandlers.items():
            self.ecsMap[pca[0]] = pca[1].stateMap.map
        self.ecsMap["unmapped"] = ecs.unmappedStateTable.map
        self.ecsObject = pcaModel.objects.filter(id="ecs").get()
        #check if another user has control over pca
        self.userInControl = None
        usersWithPermission = get_users_with_perms(self.pcaObject, attach_perms = True)
        for user, perms in usersWithPermission.items():
            if "has_control" in perms:
                self.userInControl = user
                break
        if self.userInControl == request.user:
            self.userInControl = "You"
        return super().dispatch(request, *args, **kwargs)

class pcaView(ecsMixin,TemplateView):
    template_name = "GUI/monitor.html"

    def dispatch(self, request, *args, **kwargs):
        self.partitions = []
        for pca in ecs.pcaHandlers:
            self.partitions.append(pca)
        self.pcaId = self.kwargs['pcaId']
        self.pcaObject = pcaModel.objects.filter(id=self.pcaId).get()
        self.ecsObject = pcaModel.objects.filter(id="ecs").get()
        self.userInControl = None
        usersWithPermission = get_users_with_perms(self.pcaObject, attach_perms = True)
        for user, perms in usersWithPermission.items():
            if "has_control" in perms:
                self.userInControl = user
                break
        if self.userInControl == request.user:
            self.userInControl = "You"
        return super().dispatch(request, *args, **kwargs)

class ecsPermissionMixin(PermissionRequiredMixin):
    return_403 = True
    permission_required = 'has_control'
    permission_object = pcaModel.objects.filter(id="ecs").get()


class create_pca(ecsMixin,ecsPermissionMixin,FormView):
    #FormView
    template_name = 'GUI/ECS_Create_Partition.html'
    form_class = pcaForm
    success_url = '/'
    errorMessage = None

    raise_exception = True
    def dispatch(self, request, *args, **kwargs):
        self.partitions = ecs.pcaHandlers.items()
        return super().dispatch(request, *args, **kwargs)

    def form_valid(self, form):
        if not form.is_valid():
            return super().form_invalid(form)
        values = {
                "id" :  form.cleaned_data["id"],
                "address":  form.cleaned_data["address"],
                "portPublish" :  form.cleaned_data["portPublish"],
                "portLog" :  form.cleaned_data["portLog"],
                "portUpdates" :  form.cleaned_data["portUpdates"],
                "portCurrentState" :  form.cleaned_data["portCurrentState"],
                "portSingleRequest" :  form.cleaned_data["portSingleRequest"],
                "portCommand" :  form.cleaned_data["portCommand"],
        }
        obj = partitionDataObject(values)
        ret = ecs.createPartition(obj)
        if ret == True:
            return super().form_valid(form)
        else:
            self.errorMessage = ret
            return super().form_invalid(form)

class delete_pca(ecsMixin,ecsPermissionMixin,TemplateView):
    template_name = 'GUI/ECS_Delete_Partition.html'
    raise_exception = True

    def dispatch(self, request, *args, **kwargs):
        self.partitions = ecs.pcaHandlers.items()
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        partitions = request.POST.getlist("selectedPartitions")
        self.failedPartitions = False
        for pcaId in partitions:
            detectors = ecs.getDetectorListForPartition(pcaId)
            self.forceDelete = False
            if "forceDelete" in request.POST:
                self.forceDelete = True
            #unmap detectors
            self.failedDetectors = False
            for d in detectors:
                ret = ecs.moveDetector(d.id,"unmapped",self.forceDelete)
                if ret != True:
                    if not self.failedDetectors:
                        self.failedDetectors = {}
                    self.failedDetectors[d.id] = ret
            if self.failedDetectors:
                if not self.failedPartitions:
                    self.failedPartitions = {}
                self.failedPartitions[pcaId] = ("unmapFail","Not all assigned Detectors could be unmapped")
                continue
            ret = ecs.deletePartition(pcaId,self.forceDelete)
            if ret != True:
                if not self.failedPartitions:
                    self.failedPartitions = {}
                self.failedPartitions[pcaId] = ("deleteFail",ret,)
        if self.failedPartitions:
            return self.get(request, *args, **kwargs)
        return HttpResponseRedirect('/')




@login_required
def input_edit_pca(request):
    id = request.POST["id"]
    partition = ecs.getPartition(id)
    unmappedDetectors = ecs.getUnmappedDetectors()
    return render(request, 'GUI/ECS_Edit_Partition.html', {"pca":partition, "unmappedDetectors" : unmappedDetectors})

@login_required
def edit_pca(request):
    pass

class create_detector(ecsMixin,ecsPermissionMixin,FormView):
    #FormView
    template_name = 'GUI/ECS_Create_Detector.html'
    form_class = detectorForm
    success_url = '/'
    errorMessage = None

    raise_exception = True

    def form_valid(self, form):
        if not form.is_valid():
            return super().form_invalid(form)
        values = {
                "id" : form.cleaned_data["id"],
                "address": form.cleaned_data["address"],
                "type" : form.cleaned_data["type"],
                "portTransition" : form.cleaned_data["portTransition"],
                "portCommand" : form.cleaned_data["portCommand"],
        }
        obj = detectorDataObject(values)
        ret = ecs.createDetector(obj)
        if ret == True:
            return super().form_valid(form)
        else:
            self.errorMessage = ret
            return super().form_invalid(form)

class moveDetectors(ecsMixin,ecsPermissionMixin,TemplateView):
        template_name = 'GUI/ECS_moveDetectors.html'
        raise_exception = True

        def dispatch(self, request, *args, **kwargs):
            self.unmappedDetectors = ecs.getUnmappedDetectors()
            self.partitions = ecs.pcaHandlers.items()
            return super().dispatch(request, *args, **kwargs)

        def post(self, request, *args, **kwargs):
            self.toPcaId = request.POST['toPartition']
            detectors = request.POST.getlist("selectedDetectors")
            self.forceMove = False
            if "forceMove" in request.POST:
                self.forceMove = True
            self.failedDetectors = False
            for dId in detectors:
                ret = ecs.moveDetector(dId,self.toPcaId,self.forceMove)
                if ret != True:
                    if not self.failedDetectors:
                        self.failedDetectors = {}
                    self.failedDetectors[dId] = ret
            if self.failedDetectors:
                return self.get(request, *args, **kwargs)
            return HttpResponseRedirect('/')

class deleteDetector(ecsMixin,ecsPermissionMixin,TemplateView):
    template_name = 'GUI/ECS_delete_Detectors.html'
    raise_exception = True

    def dispatch(self, request, *args, **kwargs):
        self.unmappedDetectors = ecs.getUnmappedDetectors()
        self.partitions = ecs.pcaHandlers.items()
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        detectors = request.POST.getlist("selectedDetectors")
        self.failedDetectors = False
        self.forceDelete = False
        if "forceDelete" in request.POST:
            self.forceDelete = True
        for detectorId in detectors:
            ret = ecs.deleteDetector(detectorId,self.forceDelete)
            if ret != True:
                if not self.failedDetectors:
                    self.failedDetectors = {}
                self.failedDetectors[detectorId] = ret
        if self.failedDetectors:
            return self.get(request, *args, **kwargs)
        return HttpResponseRedirect('/')

class pcaPermissionMixin(PermissionRequiredMixin):
    #PermissionRequiredMixin
    return_403 = True
    permission_required = 'has_control'

    def dispatch(self, request, *args, **kwargs):
        pcaId = self.kwargs['pcaId']
        self.permission_object = pcaModel.objects.filter(id=kwargs['pcaId']).get()
        return super().dispatch(request, *args, **kwargs)

class ready(ecsMixin,pcaPermissionMixin,View):
    raise_exception = True
    def post(self, request, *args, **kwargs):
        pcaId = self.kwargs['pcaId']
        pca = ecs.getPCAHandler(pcaId)
        pca.sendCommand(codes.getReady)
        return HttpResponse(status=200)

class start(ecsMixin,pcaPermissionMixin,View):
    raise_exception = True
    def post(self, request, *args, **kwargs):
        pcaId = self.kwargs['pcaId']
        pca = ecs.getPCAHandler(pcaId)
        pca.sendCommand(codes.start)
        return HttpResponse(status=200)

class stop(ecsMixin,pcaPermissionMixin,View):
    raise_exception = True
    def post(self, request, *args, **kwargs):
        pcaId = self.kwargs['pcaId']
        pca = ecs.getPCAHandler(pcaId)
        pca.sendCommand(codes.stop)
        return HttpResponse(status=200)

class abort(ecsMixin,pcaPermissionMixin,View):
    raise_exception = True
    def post(self, request, *args, **kwargs):
        pcaId = self.kwargs['pcaId']
        #detectorId = request.POST["detectorId"]
        pca = ecs.getPCAHandler(pcaId)
        pca.sendCommand(codes.abort)
        return HttpResponse(status=200)

class setActive(ecsMixin,pcaPermissionMixin,View):
    raise_exception = True
    def post(self, request, *args, **kwargs):
        pcaId = self.kwargs['pcaId']
        detectorId = request.POST["detectorId"]
        pca = ecs.getPCAHandler(pcaId)
        pca.sendCommand(codes.setActive,detectorId)
        return HttpResponse(status=200)

class setInactive(ecsMixin,pcaPermissionMixin,View):
    raise_exception = True
    def post(self, request, *args, **kwargs):
        pcaId = self.kwargs['pcaId']
        detectorId = request.POST["detectorId"]
        pca = ecs.getPCAHandler(pcaId)
        pca.sendCommand(codes.setInactive,detectorId)
        return HttpResponse(status=200)

@login_required
def getDetectorListForPCA(request):
    """Ask ECS for DetectorList from Database"""
    pcaId = request.POST['pcaId']
    detList = ecs.getDetectorListForPartition(pcaId)
    if detList == codes.error:
        return HttpResponse(status=404)
    else:
        return JsonResponse(detList.asDictionary())

@login_required
def getUnmappedDetectors(request):
    """Ask ECS for DetectorList from Database"""
    detList = ecs.ecs.getUnmappedDetectors()
    if detList == codes.error:
        return HttpResponse(status=404)
    else:
        return JsonResponse(detList.asDictionary())

@login_required
def currentTableAndLogRequest(request,pcaId):
    """get Log Data and current Statetable on Websocket connect"""
    bufferdLog = False
    #convert stateObjects to Json
    f = lambda x:(x[0],x[1].asJson())
    if pcaId == "ecs":
        #all pcas for ECS Overview
        map = {}
        for pca in ecs.pcaHandlers.items():
            map[pca[0]] = dict((k,f(v)) for k,v in pca[1].stateMap.map.items())
        map["unmapped"] = dict((k,f(v)) for k,v in ecs.unmappedStateTable.map.items())
        #map =  json.dumps(map, default=lambda o: o.__dict__)
        #map = json.dumps(map)
        if len(ecs.logQueue) > 0:
            bufferdLog = "\n".join(list(ecs.logQueue))
    else:
        #single pca
        handler = ecs.pcaHandlers[pcaId]
        map = dict((k,f(v)) for k,v in handler.stateMap.map.items())#json.dumps(handler.stateMap.map, default=lambda o: o.__dict__)
        #send buffered log entries
        if len(handler.logQueue) > 0:
            bufferdLog = "\n".join(list(handler.logQueue))

    response = {
        "table" : map,
        "log" : bufferdLog,
    }
    return JsonResponse(response)

@permission_required('GUI.can_take_control')
@login_required
def takeControl(request,pcaId):
    pcaObject = pcaModel.objects.filter(id=pcaId).get()
    if pcaId == "ecs":
        redirect = HttpResponseRedirect('/',{"pcaList" : ecs.pcaHandlers.items()})
    else:
        pca = ecs.getPCAHandler(pcaId)
        redirect = HttpResponseRedirect("/pca/"+pcaId,{'stateMap': pca.stateMap.map, "pcaId" : pcaId, "pcaObject" : pcaObject})
    #check if other user has control for pca
    usersWithPermission = get_users_with_perms(pcaObject, attach_perms = True)
    for user, perms in usersWithPermission.items():
        if "has_control" in perms:
            return redirect
    assign_perm('has_control', request.user, pcaObject)
    pcaObject.permissionTimestamp = timezone.now()
    pcaObject.save()
    return redirect

@permission_required('GUI.can_take_control')
@login_required
def giveUpControl(request,pcaId):
    pcaObject = pcaModel.objects.filter(id=pcaId).get()
    if pcaId == "ecs":
        redirect = HttpResponseRedirect('/',{"pcaList" : ecs.pcaHandlers.items()})
    else:
        pca = ecs.getPCAHandler(pcaId)
        redirect = HttpResponseRedirect("/pca/"+pcaId,{'stateMap': pca.stateMap.map, "pcaId" : pcaId, "pcaObject" : pcaObject})

    remove_perm('has_control', request.user, pcaObject)
    return redirect
