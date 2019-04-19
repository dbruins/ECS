from django.shortcuts import get_object_or_404,render,redirect
from guardian.shortcuts import assign_perm, get_users_with_perms, remove_perm, get_user_perms
from django.http import HttpResponse, HttpResponseRedirect, JsonResponse
from GUI.models import pcaModel, LoggedInUser, ecsModel
from django.urls import reverse
from django.conf import settings
import json
from django.contrib.auth.decorators import login_required, permission_required
from django.contrib.auth.models import Permission
from django.apps import apps
import threading
import time
from django.utils import timezone
from django import forms
import sys

projectPath = settings.PATH_TO_PROJECT
sys.path.append(projectPath)
from ECSCodes import ECSCodes
codes = ECSCodes()
from DataObjects import DataObjectCollection, detectorDataObject, partitionDataObject, stateObject, configObject
from ECA import ECA

eca = ECA()

gui = apps.get_app_config('GUI')

from django.contrib.auth import user_logged_out, user_logged_in
from django.dispatch import receiver

@receiver(user_logged_in)
def on_user_logged_in(sender, request, **kwargs):
    """trigger for user login"""
    LoggedInUser.objects.get_or_create(user=request.user)

@receiver(user_logged_out)
def on_user_logged_out(sender, request, **kwargs):
    """frees taken permissions on user logout"""

    if request.user.is_authenticated:
        LoggedInUser.objects.filter(user=request.user).delete()
    #pcas
    for pca in eca.pcaHandlers.items():
        pcaObject = pcaModel.objects.filter(id=pca[0]).get()
        if request.user.has_perm("has_pca_control",pcaObject):
         remove_perm('has_pca_control', request.user, pcaObject)
    #ecs
    ecsObject = ecsModel.objects.filter(id="ecs").get()
    if request.user.has_perm("has_ecs_control",ecsObject):
        remove_perm('has_ecs_control', request.user, ecsObject)

def permission_timeout():
    """remove permission has control if user has been inactive for a while"""
    while True:
        time.sleep(settings.PERMISSION_TIMEOUT)
        allPCAs = pcaModel.objects.all()
        #check for all pca
        for pcaObject in allPCAs:
            usersWithPermission = get_users_with_perms(pcaObject, attach_perms = True)
            for user, perms in usersWithPermission.items():
                if "has_pca_control" in perms:
                    timedelta = timezone.now() - pcaObject.permissionTimestamp
                    #check if there is a timeout
                    if timedelta.total_seconds() > settings.PERMISSION_TIMEOUT:
                        print(str(timedelta.total_seconds()) +" timeout user: "+str(user) )
                        remove_perm('has_pca_control', user, pcaObject)
                        #inform over websocket
                        eca.webSocket.permissionTimeout(str(user))
        #check for ecs
        ecsObject = ecsModel.objects.filter(id="ecs").get()
        usersWithPermission = get_users_with_perms(ecsObject, attach_perms = True)
        for user, perms in usersWithPermission.items():
            if "has_ecs_control" in perms:
                timedelta = timezone.now() - ecsObject.permissionTimestamp
                if timedelta.total_seconds() > settings.PERMISSION_TIMEOUT:
                    print(str(timedelta.total_seconds()) +" timeout user: "+str(user) )
                    remove_perm('has_ecs_control', user, ecsObject)
                    #inform over websocket
                    eca.webSocket.permissionTimeout(str(user))

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
            if 'has_pca_control' in perm:
                pcaObject.permissionTimestamp = timezone.now()
                pcaObject.save()
        ecsObject = ecsModel.objects.filter(id="ecs").get()
        perm = get_user_perms(request.user,ecsObject)
        if 'has_ecs_control' in perm:
            ecsObject.permissionTimestamp = timezone.now()
            ecsObject.save()
        return super().dispatch(request, *args, **kwargs)

class ecsMixin(LoginRequiredMixin,resetUserTimeoutMixin):
    """combination of Login required and reset timeout """
    partitions = eca.pcaHandlers.items()
    ecsObject = ecsModel.objects.filter(id="ecs").get()

class pcaForm(forms.Form):
    id = forms.CharField(label="id")
    address = forms.CharField(label="address")
    portPublish = forms.IntegerField(min_value= 1,label="Publish Port")
    portLog = forms.IntegerField(min_value= 1,label="Port Logmessages")
    portUpdates = forms.IntegerField(min_value= 1,label="Port Updates")
    portCurrentState = forms.IntegerField(min_value= 1,label="Port Current State")
    portCommand = forms.IntegerField(min_value= 1,label="Port Command")

class detectorForm(forms.Form):
    id = forms.CharField(label="id")
    address = forms.CharField(label="address")
    type = forms.CharField(label="type")
    portCommand = forms.IntegerField(min_value= 1,label="Port Command")

#views
class index(ecsMixin,TemplateView):
    template_name = "GUI/index.html"
    ecsMap = {}
    userInControl = None
    webSocketPort = settings.WEB_SOCKET_PORT

    def dispatch(self, request, *args, **kwargs):
        self.ecsMap = {}
        self.usersInPCAControl = {}
        for pca in eca.pcaHandlers.items():
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
        self.ecsMap["unmapped"] = eca.unmappedDetectorController.statusMap.map
        #check if another user has control over ecs
        self.userInControl = None
        #get users who have control for ecs
        users = get_users_with_perms(self.ecsObject, attach_perms = True)
        if len(users)>0:
            #there should only be at most one who is controlling partition/ecs
            self.userInControl = list(users.keys())[0]
        if self.userInControl == request.user:
            self.userInControl = "You"
        if hasattr(request.user, 'logged_in_user'):
            self.sessionId = request.user.logged_in_user.session_key
        return super().dispatch(request, *args, **kwargs)

class pcaView(ecsMixin,TemplateView):
    template_name = "GUI/monitor.html"
    webSocketPort = settings.WEB_SOCKET_PORT

    def dispatch(self, request, *args, **kwargs):
        self.pcaId = self.kwargs['pcaId']
        self.pcaObject = pcaModel.objects.filter(id=self.pcaId).get()
        self.userInPCAControl = None
        usersWithPermission = get_users_with_perms(self.pcaObject, attach_perms = True)
        for user, perms in usersWithPermission.items():
            if "has_pca_control" in perms:
                self.userInPCAControl = user
                break
        if self.userInPCAControl == request.user:
            self.userInPCAControl = "You"

        self.userInECSControl = None
        usersWithPermission = get_users_with_perms(self.ecsObject, attach_perms = True)
        for user, perms in usersWithPermission.items():
            if "has_ecs_control" in perms:
                self.userInECSControl = user
                break
        if self.userInECSControl == request.user:
            self.userInECSControl = "You"
        if hasattr(request.user, 'logged_in_user'):
            self.sessionId = request.user.logged_in_user.session_key
        return super().dispatch(request, *args, **kwargs)

class ecsPermissionMixin(PermissionRequiredMixin):
    return_403 = True
    permission_required = 'has_ecs_control'
    permission_object = ecsModel.objects.filter(id="ecs").get()


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
                "portCommand" :  form.cleaned_data["portCommand"],
        }
        obj = partitionDataObject(values)
        ret = eca.createPartition(obj)
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
            detectors = eca.database.getDetectorsForPartition(pcaId)
            self.forceDelete = False
            if "forceDelete" in request.POST:
                self.forceDelete = True
            #unmap detectors
            self.failedDetectors = False
            for d in detectors:
                ret = eca.moveDetector(d.id,"unmapped",self.forceDelete)
                if ret != True:
                    if not self.failedDetectors:
                        self.failedDetectors = {}
                    self.failedDetectors[d.id] = ret
            if self.failedDetectors:
                if not self.failedPartitions:
                    self.failedPartitions = {}
                self.failedPartitions[pcaId] = ("unmapFail","Not all assigned Detectors could be unmapped")
                continue
            ret = eca.deletePartition(pcaId,self.forceDelete)
            if ret != True:
                if not self.failedPartitions:
                    self.failedPartitions = {}
                self.failedPartitions[pcaId] = ("deleteFail",ret,)
        if self.failedPartitions:
            return self.get(request, *args, **kwargs)
        return HttpResponseRedirect('/')

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
                "portCommand" : form.cleaned_data["portCommand"],
        }
        obj = detectorDataObject(values)
        ret = eca.createDetector(obj)
        if ret == True:
            return super().form_valid(form)
        else:
            self.errorMessage = ret
            return super().form_invalid(form)

class moveDetectors(ecsMixin,ecsPermissionMixin,TemplateView):
        template_name = 'GUI/ECS_moveDetectors.html'
        raise_exception = True

        def dispatch(self, request, *args, **kwargs):
            self.unmappedDetectors = eca.database.getAllUnmappedDetectors()
            return super().dispatch(request, *args, **kwargs)

        def post(self, request, *args, **kwargs):
            self.toPcaId = request.POST['toPartition']
            detectors = request.POST.getlist("selectedDetectors")
            self.forceMove = False
            if "forceMove" in request.POST:
                self.forceMove = True
            self.failedDetectors = False
            for dId in detectors:
                ret = eca.moveDetector(dId,self.toPcaId,self.forceMove)
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
        self.unmappedDetectors = eca.database.getAllUnmappedDetectors()
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        detectors = request.POST.getlist("selectedDetectors")
        self.failedDetectors = False
        self.forceDelete = False
        if "forceDelete" in request.POST:
            self.forceDelete = True
        for detectorId in detectors:
            ret = eca.deleteDetector(detectorId,self.forceDelete)
            if ret != True:
                if not self.failedDetectors:
                    self.failedDetectors = {}
                self.failedDetectors[detectorId] = ret
        if self.failedDetectors:
            return self.get(request, *args, **kwargs)
        return HttpResponseRedirect('/')

class editConfiguration(ecsMixin,ecsPermissionMixin,TemplateView):
    template_name = 'GUI/editConfig.html'
    raise_exception = True

    def dispatch(self, request, *args, **kwargs):
        """get the webpage"""
        res = eca.getAllSystems()
        self.detectors = list(map(lambda x:x.id,res["detectors"].dataArray))
        self.globalSystems = list(map(lambda x:x.id,res["globalSystems"].dataArray))

        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        """saves selected system configuration as new tag in the database"""
        #check if all needed values are there
        if {'configId', 'systemId',"paramList"}.issubset( request.POST ):
            configId = request.POST['configId']
            systemId = request.POST['systemId']
            params = request.POST["paramList"]
            ret = eca.database.saveConfig(configId,systemId,params)
        elif {'configId', 'delete'}.issubset( request.POST ):
            configId = request.POST['configId']
            ret = eca.database.deleteConfig(configId)
        else:
            return HttpResponse(status=406)
        if isinstance(ret,Exception):
            return HttpResponse(status=500)
        else:
            return HttpResponse(status=200)

class editConfigurationTag(ecsMixin,ecsPermissionMixin,TemplateView):
    template_name = 'GUI/editTag.html'
    raise_exception = True

    def dispatch(self, request, *args, **kwargs):
        """get the webpage"""
        res = eca.getAllSystems()
        #get ids of all systems
        self.detectors = list(map(lambda x:x.id,res["detectors"].dataArray))
        self.globalSystems = list(map(lambda x:x.id,res["globalSystems"].dataArray))

        #get all configs for all
        self.tags = eca.database.getAllConfigTags()
        globalSystemConfigs = eca.database.getConfigsForManySystems(self.globalSystems)
        self.gobalConfigsForSystem = {}
        for config in globalSystemConfigs:
            if config.systemId not in self.gobalConfigsForSystem:
                self.gobalConfigsForSystem[config.systemId] = []
            self.gobalConfigsForSystem[config.systemId].append(config)
        detectorConfigs = eca.database.getConfigsForManySystems(self.detectors)
        self.detectorConfigsForSystem = {}
        for config in detectorConfigs:
            if config.systemId not in self.detectorConfigsForSystem:
                self.detectorConfigsForSystem[config.systemId] = []
            self.detectorConfigsForSystem[config.systemId].append(config)
        return super().dispatch(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        """saves selected system configuration as new tag in the database"""
        if {'tagName', 'configList[]'}.issubset( request.POST ):
            tagName = request.POST['tagName']
            configs = request.POST.getlist("configList[]")
            ret = eca.database.saveConfigTag(tagName,configs)
        elif {'tagName', 'delete'}.issubset( request.POST ):
            tagName = request.POST['tagName']
            ret = eca.database.deleteConfigTag(tagName)
        else:
            return HttpResponse(status=406)
        if isinstance(ret,Exception):
            return HttpResponse(status=500)
        else:
            return HttpResponse(status=200)


class clientPage(ecsMixin,ecsPermissionMixin,TemplateView):
    template_name = 'GUI/clients.html'
    raise_exception = True

    def get(self, request, *args, **kwargs):
        self.pcas = {}
        self.detectors = {}
        for pca in eca.pcaHandlers.items():
            self.pcas[pca[0]] = eca.checkIfRunning(eca.partitions[pca[0]])
            for d in eca.database.getDetectorsForPartition(pca[0]):
                self.detectors[d.id] = eca.checkIfRunning(d)
        self.globalSystems = {}
        for gs in eca.globalSystems.items():
            self.globalSystems[gs[0]] = eca.checkIfRunning(eca.globalSystems[gs[0]])
        return super().get(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        id = request.POST['id']
        action = request.POST['action']
        if request.POST['type'] == "partition":
            object = eca.partitions[id]
        elif request.POST['type'] == "detector":
            object = eca.database.getDetector(id)
        elif request.POST['type'] == "global":
            object = eca.globalSystems[id]
        else:
            return HttpResponse(status=500)
        if action == "start":
            pid = eca.startClient(object)
            if not pid:
                return HttpResponse(status=500)
            else:
                return JsonResponse({"pid":pid,})
        elif action == "stop":
            ret = eca.stopClient(object)
            if ret:
                return HttpResponse(status=200)
            else:
                return HttpResponse(status=500)
        else:
            return HttpResponse(status=500)

class pcaPermissionMixin(PermissionRequiredMixin):
    #PermissionRequiredMixin for PCAs
    return_403 = True
    permission_required = 'has_pca_control'

    def dispatch(self, request, *args, **kwargs):
        pcaId = self.kwargs['pcaId']
        self.permission_object = pcaModel.objects.filter(id=kwargs['pcaId']).get()
        return super().dispatch(request, *args, **kwargs)

class ready(ecsMixin,pcaPermissionMixin,View):
    raise_exception = True
    def post(self, request, *args, **kwargs):
        pcaId = self.kwargs['pcaId']
        autoConfigure = request.POST['autoConfigure']
        globalTag = request.POST['globalTag']
        pca = eca.getPCAHandler(pcaId)

        if "customConfiguration[]" in request.POST:
            #get custom configuration
            customConfiguration = request.POST.getlist("customConfiguration[]")
            systemConfig =eca.database.getCustomConfig(customConfiguration)
        else:
            systemConfig = eca.database.getConfigsForTag(globalTag)
        if systemConfig == codes.idUnknown:
            pca.log("tag %s not found" % globalTag)
            return HttpResponse(status=500)

        arg = {
            "globalTag" : globalTag,
            "systemConfig" : systemConfig.asJsonString(),
            "autoConfigure": autoConfigure,
        }
        pca.sendCommand(codes.getReady,json.dumps(arg))
        return HttpResponse(status=200)

class start(ecsMixin,pcaPermissionMixin,View):
    raise_exception = True
    def post(self, request, *args, **kwargs):
        pcaId = self.kwargs['pcaId']
        pca = eca.getPCAHandler(pcaId)
        pca.sendCommand(codes.start)
        return HttpResponse(status=200)

class stop(ecsMixin,pcaPermissionMixin,View):
    raise_exception = True
    def post(self, request, *args, **kwargs):
        pcaId = self.kwargs['pcaId']
        pca = eca.getPCAHandler(pcaId)
        pca.sendCommand(codes.stop)
        return HttpResponse(status=200)

class abort(ecsMixin,pcaPermissionMixin,View):
    raise_exception = True
    def post(self, request, *args, **kwargs):
        pcaId = self.kwargs['pcaId']
        pca = eca.getPCAHandler(pcaId)
        pca.sendCommand(codes.abort)
        return HttpResponse(status=200)

class reset(ecsMixin,pcaPermissionMixin,View):
    raise_exception = True
    def post(self, request, *args, **kwargs):
        pcaId = self.kwargs['pcaId']
        systemId = request.POST['systemId']
        pca = eca.getPCAHandler(pcaId)
        arg = {
            "systemId" : systemId,
        }
        pca.sendCommand(codes.reset,json.dumps(arg))
        return HttpResponse(status=200)

class configTagModalView(ecsMixin,pcaPermissionMixin,TemplateView):
    template_name = "GUI/config_modal.html"
    raise_exception = True

    def dispatch(self, request, *args, **kwargs):
        pcaId = self.kwargs['pcaId']
        handler = eca.getPCAHandler(pcaId)
        tag = handler.stateMap[pcaId][1].configTag
        configList = eca.database.getConfigsForPCA(pcaId)
        if isinstance(configList,Exception):
            self.error = str(configList)
            return super().dispatch(request, *args, **kwargs)
        if not tag:
            systemConfigForTag = {}
        else:
            self.currentTag=tag
            systemConfigForTag = eca.database.getConfigsForTag(tag)

        if isinstance(systemConfigForTag,Exception):
            self.error = str(systemConfigForTag)
            return super().dispatch(request, *args, **kwargs)

        #config for currently selected tag
        self.systemConfigs = {}
        if systemConfigForTag == codes.idUnknown:
            #custom tag get config from stateMap
            pcaTagList = []
            self.customTag = True
            for sysId in handler.stateMap.keyIterator():
                state = handler.stateMap[sysId][1]
                if state.configTag and sysId != pcaId:
                    pcaTagList.append(state.configTag)
            systemConfigForTag =eca.database.getCustomConfig(pcaTagList)
        else:
            self.customTag = False
        for c in systemConfigForTag:
            self.systemConfigs[c.systemId] =  {c.configId:c.parameters}

        #list for systems without configs
        self.systemsWithoutConfig = []
        #all available configs other than the allready selected
        for c in configList:
            if c.configId == None:
                self.systemsWithoutConfig.append(c.systemId)
                continue
            if c.systemId not in self.systemConfigs:
                self.systemConfigs[c.systemId] = {}
            if c.configId not in self.systemConfigs[c.systemId]:
                self.systemConfigs[c.systemId][c.configId] = c.parameters

        tagList = eca.database.getPcaCompatibleTags(pcaId)
        if tagList:
            if isinstance(tagList,Exception):
                self.error = str(tagList)
            else:
                self.tagList = tagList

        return super().dispatch(request, *args, **kwargs)
@login_required
def getDetectorListForPCA(request):
    """Ask ECS for DetectorList from Database for a PCA"""
    pcaId = request.POST['pcaId']
    detList = eca.database.getDetectorsForPartition(pcaId)
    if detList == codes.error:
        return HttpResponse(status=404)
    else:
        return JsonResponse(detList.asDictionary())

@login_required
def getConfigsForSystem(request):
    """Ask ECS for Configurations from Database for a System"""
    id = request.POST['id']
    configList = eca.database.getConfigsForSystem(id)
    if configList == codes.idUnknown:
        return HttpResponse(status=404)
    else:
        return JsonResponse(configList.asDictionary())

@login_required
def getConfigsForTag(request):
    """Ask ECS for DetectorList from Database for a Tag"""
    tag = request.POST['tag']
    configList = eca.database.getConfigsForTag(tag)
    if configList == codes.idUnknown:
        configList = {}
    else:
        configList = configList.asDictionary()

    if isinstance(configList,Exception):
        return JsonResponse({"error":str(configList)})
    return JsonResponse(configList)

@login_required
def getUnmappedDetectors(request):
    """Ask ECS for DetectorList from Database"""
    detList = eca.database.getAllUnmappedDetectors()
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
        for pca in eca.pcaHandlers.items():
            map[pca[0]] = dict((k,(v[0],v[1].asJson(),k in eca.globalSystems)) for k,v in pca[1].stateMap.map.items())
        eca.unmappedDetectorController.statusMap.map.items()
        map["unmapped"] = dict((k,(v[0],v[1].asJson(),k in eca.globalSystems)) for k,v in eca.unmappedDetectorController.statusMap.map.items())
        if len(eca.logQueue) > 0:
            bufferdLog = "\n".join(list(eca.logQueue))
        response = {
            "table" : map,
            "log" : bufferdLog,
        }
    else:
        #single pca
        if pcaId in eca.pcaHandlers:
            handler = eca.pcaHandlers[pcaId]
            map = dict((k,(v[0],v[1].asJson(),k in eca.globalSystems)) for k,v in handler.stateMap.map.items())
            #send buffered log entries
            if len(handler.logQueue) > 0:
                bufferdLog = "\n".join(list(handler.logQueue))
            if pcaId in handler.stateMap.map:
                buttons = PCAStates.UIButtonsForState(handler.stateMap.map[pcaId][1].state)
            else:
                buttons = {}
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

@permission_required('GUI.can_take_pca_control')
@login_required
def takePCAControl(request,pcaId,targetPage):
    if targetPage == "ecs":
        redirect = HttpResponseRedirect('/',{"pcaList" : eca.pcaHandlers.items()})
    else:
        pca = eca.getPCAHandler(targetPage)
        redirect = HttpResponseRedirect("/pca/"+targetPage,{'stateMap': pca.stateMap.map, "pcaId" : targetPage, "pcaObject" : pcaModel.objects.filter(id=targetPage).get()})
    #check if other user has control for ecs
    object = pcaModel.objects.filter(id=pcaId).get()
    usersWithPermission = get_users_with_perms(object, attach_perms = True)
    for user, perms in usersWithPermission.items():
        if "has_pca_control" in perms:
            return redirect
    assign_perm("has_pca_control", request.user, object)
    object.permissionTimestamp = timezone.now()
    object.save()
    return redirect

@permission_required('GUI.can_take_ecs_control')
@login_required
def takeECSControl(request,targetPage):
    if targetPage == "ecs":
        redirect = HttpResponseRedirect('/',{"pcaList" : eca.pcaHandlers.items()})
    else:
        pca = eca.getPCAHandler(targetPage)
        redirect = HttpResponseRedirect("/pca/"+targetPage,{'stateMap': pca.stateMap.map, "pcaId" : targetPage, "pcaObject" : pcaModel.objects.filter(id=targetPage).get()})
    #check if other user has control for ecs
    object = ecsModel.objects.filter(id="ecs").get()
    usersWithPermission = get_users_with_perms(object, attach_perms = True)
    for user, perms in usersWithPermission.items():
        if "has_ecs_control" in perms:
            return redirect
    assign_perm("has_ecs_control", request.user, object)
    object.permissionTimestamp = timezone.now()
    object.save()
    return redirect

@permission_required('GUI.can_take_pca_control')
@login_required
def giveUpPCAControl(request,pcaId,targetPage):
    if targetPage == "ecs":
        redirect = HttpResponseRedirect('/',{"pcaList" : eca.pcaHandlers.items()})
    else:
        pca = eca.getPCAHandler(targetPage)
        redirect = HttpResponseRedirect("/pca/"+targetPage,{'stateMap': pca.stateMap.map, "pcaId" : targetPage, "pcaObject" : pcaModel.objects.filter(id=targetPage).get()})
    object = pcaModel.objects.filter(id=pcaId).get()
    remove_perm("has_pca_control", request.user, object)
    return redirect

@permission_required('GUI.can_take_ecs_control')
@login_required
def giveUpECSControl(request,targetPage):
    if targetPage == "ecs":
        redirect = HttpResponseRedirect('/',{"pcaList" : eca.pcaHandlers.items()})
    else:
        pca = eca.getPCAHandler(targetPage)
        redirect = HttpResponseRedirect("/pca/"+targetPage,{'stateMap': pca.stateMap.map, "pcaId" : targetPage, "pcaObject" : pcaModel.objects.filter(id=targetPage).get()})
    object = ecsModel.objects.filter(id="ecs").get()
    remove_perm("has_ecs_control", request.user, object)
    return redirect
