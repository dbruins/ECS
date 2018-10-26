from django.shortcuts import get_object_or_404,render,redirect
from guardian.shortcuts import assign_perm, get_users_with_perms, remove_perm, get_user_perms
from django.http import HttpResponse, HttpResponseRedirect, JsonResponse
from GUI.models import pcaModel
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
from multiprocessing import Queue
import time
from django.utils import timezone
from django import forms
import sys
#ECS Codes and DataObjects should be in the same Path later
projectPath = settings.PATH_TO_PROJECT
sys.path.append(projectPath)
from ECSCodes import ECSCodes
codes = ECSCodes()
from DataObjects import DataObjectCollection, detectorDataObject, partitionDataObject, stateObject
from ECS import ECS,DataBaseWrapper

ecs = ECS()

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
    partitions = ecs.pcaHandlers.items()
    ecsObject = pcaModel.objects.filter(id="ecs").get()

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
    userInControl = None

    def dispatch(self, request, *args, **kwargs):
        self.ecsMap = {}
        self.usersInPCAControl = {}
        for pca in ecs.pcaHandlers.items():
            self.ecsMap[pca[0]] = pca[1].stateMap.map
            #get users who have control for PCA
            users = get_users_with_perms(pcaModel.objects.filter(id=pca[0]).get(), attach_perms = True)
            if len(users)>0:
                #there should only be at most one who is controlling partition/ecs
                user = list(users.keys())[0]
                if user == request.user:
                    self.usersInPCAControl[pca[0]] = "You"
                else:
                    self.usersInPCAControl[pca[0]]=user
            else:
                self.usersInPCAControl[pca[0]]=None
        self.ecsMap["unmapped"] = ecs.unmappedDetectorController.statusMap.map
        #check if another user has control over pca
        self.userInControl = None
        #get users who have control for ecs
        users = get_users_with_perms(self.ecsObject, attach_perms = True)
        if len(users)>0:
            #there should only be at most one who is controlling partition/ecs
            self.userInControl = list(users.keys())[0]
        if self.userInControl == request.user:
            self.userInControl = "You"
        return super().dispatch(request, *args, **kwargs)

class pcaView(ecsMixin,TemplateView):
    template_name = "GUI/monitor.html"

    def dispatch(self, request, *args, **kwargs):
        self.pcaId = self.kwargs['pcaId']
        self.pcaObject = pcaModel.objects.filter(id=self.pcaId).get()
        self.userInPCAControl = None
        usersWithPermission = get_users_with_perms(self.pcaObject, attach_perms = True)
        for user, perms in usersWithPermission.items():
            if "has_control" in perms:
                self.userInPCAControl = user
                break
        if self.userInPCAControl == request.user:
            self.userInPCAControl = "You"

        self.userInControl = None
        usersWithPermission = get_users_with_perms(self.ecsObject, attach_perms = True)
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

    def post(self, request, *args, **kwargs):
        partitions = request.POST.getlist("selectedPartitions")
        self.failedPartitions = False
        for pcaId in partitions:
            detectors = ecs.getDetectorsForPartition(pcaId)
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

class clientPage(ecsMixin,ecsPermissionMixin,TemplateView):
    template_name = 'GUI/clients.html'
    raise_exception = True

    def get(self, request, *args, **kwargs):
        self.pcas = {}
        self.detectors = {}
        for pca in ecs.pcaHandlers.items():
            self.pcas[pca[0]] = ecs.checkIfRunning(ecs.partitions[pca[0]])
            for d in ecs.getDetectorsForPartition(pca[0]):
                self.detectors[d.id] = ecs.checkIfRunning(d)
        self.globalSystems = {}
        for gs in ecs.globalSystems.items():
            self.globalSystems[gs[0]] = ecs.checkIfRunning(ecs.globalSystems[gs[0]])
        return super().get(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        id = request.POST['id']
        action = request.POST['action']
        if request.POST['type'] == "partition":
            object = ecs.partitions[id]
        elif request.POST['type'] == "detector":
            object = ecs.getDetector(id)
        elif request.POST['type'] == "global":
            object = ecs.globalSystems[id]
        else:
            return HttpResponse(status=500)
        if action == "start":
            pid = ecs.startClient(object)
            if not pid:
                return HttpResponse(status=500)
            else:
                return JsonResponse({"pid":pid,})
        elif action == "stop":
            ret = ecs.stopClient(object)
            if ret:
                return HttpResponse(status=200)
            else:
                return HttpResponse(status=500)
        else:
            return HttpResponse(status=500)

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
        autoConfigure = request.POST['autoConfigure']
        configTag = request.POST['configTag']
        pca = ecs.getPCAHandler(pcaId)
        arg = {
            "configTag" : configTag,
            "autoConfigure": autoConfigure,
        }
        pca.sendCommand(codes.getReady,json.dumps(arg))
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
        pca = ecs.getPCAHandler(pcaId)
        pca.sendCommand(codes.abort)
        return HttpResponse(status=200)

@login_required
def getDetectorListForPCA(request):
    """Ask ECS for DetectorList from Database"""
    pcaId = request.POST['pcaId']
    detList = ecs.getDetectorsForPartition(pcaId)
    if detList == codes.error:
        return HttpResponse(status=404)
    else:
        return JsonResponse(detList.asDictionary())

@login_required
def getUnmappedDetectors(request):
    """Ask ECS for DetectorList from Database"""
    detList = ecs.getUnmappedDetectors()
    if detList == codes.error:
        return HttpResponse(status=404)
    else:
        return JsonResponse(detList.asDictionary())

from states import PCAStates
PCAStates = PCAStates()
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
            map[pca[0]] = dict((k,(v[0],v[1].asJson(),k in ecs.globalSystems)) for k,v in pca[1].stateMap.map.items())
        map["unmapped"] = dict((k,(v[0],v[1].asJson(),k in ecs.globalSystems)) for k,v in ecs.unmappedDetectorController.statusMap.map.items())
        if len(ecs.logQueue) > 0:
            bufferdLog = "\n".join(list(ecs.logQueue))
        response = {
            "table" : map,
            "log" : bufferdLog,
        }
    else:
        #single pca
        if pcaId in ecs.pcaHandlers:
            handler = ecs.pcaHandlers[pcaId]
            #f = lambda x:[x[0],x[1].asJson()].append(k in ecs.globalSystems)
            map = dict((k,(v[0],v[1].asJson(),k in ecs.globalSystems)) for k,v in handler.stateMap.map.items())
            #send buffered log entries
            if len(handler.logQueue) > 0:
                bufferdLog = "\n".join(list(handler.logQueue))
            buttons = PCAStates.UIButtonsForState(handler.stateMap.map[pcaId][1].state)
        else:
            bufferdLog = ""
            map = {}
            buttons = {}
        response = {
            "table" : map,
            "log" : bufferdLog,
            "buttons": buttons,
        }
    return JsonResponse(response)

@permission_required('GUI.can_take_control')
@login_required
def takeControl(request,pcaId,targetPage):
    if targetPage == "ecs":
        redirect = HttpResponseRedirect('/',{"pcaList" : ecs.pcaHandlers.items()})
    else:
        pca = ecs.getPCAHandler(targetPage)
        redirect = HttpResponseRedirect("/pca/"+targetPage,{'stateMap': pca.stateMap.map, "pcaId" : targetPage, "pcaObject" : pcaModel.objects.filter(id=targetPage).get()})
    #check if other user has control for pca
    pcaObject = pcaModel.objects.filter(id=pcaId).get()
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
def giveUpControl(request,pcaId,targetPage):
    if targetPage == "ecs":
        redirect = HttpResponseRedirect('/',{"pcaList" : ecs.pcaHandlers.items()})
    else:
        pca = ecs.getPCAHandler(targetPage)
        redirect = HttpResponseRedirect("/pca/"+targetPage,{'stateMap': pca.stateMap.map, "pcaId" : targetPage, "pcaObject" : pcaModel.objects.filter(id=targetPage).get()})
    pcaObject = pcaModel.objects.filter(id=pcaId).get()
    remove_perm('has_control', request.user, pcaObject)
    return redirect
