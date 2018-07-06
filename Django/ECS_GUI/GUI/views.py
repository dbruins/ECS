from django.shortcuts import get_object_or_404,render
from django.http import HttpResponse, HttpResponseRedirect
from GUI.models import Question, Choice
from django.template import loader
from django.urls import reverse
from django.conf import settings

import zmq
import threading
import struct
from multiprocessing import Queue
import time
import configparser

import sys
projectPath = settings.PCACODESPATH
sys.path.append(projectPath)
import ECSCodes

class PCAHandler:
    logQueue = None
    commandSocketQueue = None
    stateMap = {}

    context = None
    socketSubscription = None
    socketGetCurrentStateTable = None
    getCurrentStateTableAddress = ""
    socketSubLog  = None
    commandSocket = None
    commandSocketAdress = ""

    PCAConnection = False
    pingIntervall = 0
    receive_timeout = 0
    pingTimeout = 0

    def __init__(self):
        self.logQueue = Queue()
        self.commandSocketQueue = Queue()
        self.stateMap = {}

        self.receive_timeout = settings.TIMEOUT
        self.pingTimeout = settings.PINGTIMEOUT
        self.pingIntervall = settings.PINGINTERVALL

        config = configparser.ConfigParser()
        config.read(settings.PCACONFIGPATH)
        ports = config["ZMQPorts"]
        self.getCurrentStateTableAddress = "tcp://localhost:%s" % ports["serveCurrentStatus"]
        self.commandSocketAdress = "tcp://localhost:%s" % ports["commandSocket"]

        #state Change subscription
        self.context = zmq.Context()
        self.socketSubscription = self.context.socket(zmq.SUB)
        self.socketSubscription.connect("tcp://localhost:%s" % ports["statePublish"])
        #subscribe to everything
        self.socketSubscription.setsockopt(zmq.SUBSCRIBE, b'')
        #self.socketSubscription.setsockopt(zmq.RCVTIMEO, self.receive_timeout)

        self.createGetCurrentStateTableSocket()

        #logsubscription
        self.socketSubLog = self.context.socket(zmq.SUB)
        self.socketSubLog.connect("tcp://localhost:%s" % ports["logPublish"])
        self.socketSubLog.setsockopt(zmq.SUBSCRIBE, b'')

        self.createCommandSocket()

        self.PCAConnection = False

        t = threading.Thread(name="updater", target=self.waitForUpdates)
        self.getStateSnapshot()
        t.start()
        t = threading.Thread(name="logUpdater", target=self.waitForLogUpdates)
        t.start()
        t = threading.Thread(name="heartbeat", target=self.commandSocketHandler)
        t.start()

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
                    #self.commandSocketQueue.put(m)
                    continue
                if m != ECSCodes.ping:
                    #we've just send a message we don't need a ping
                    nextPing = time.time() + self.pingIntervall
            if time.time() > nextPing:
                self.commandSocketQueue.put(ECSCodes.ping)
                nextPing = time.time() + self.pingIntervall


    def createCommandSocket(self):
        """init or reset the command Socket"""
        if(self.commandSocket):
            #reset
            self.commandSocket.close()
        self.commandSocket = self.context.socket(zmq.REQ)
        self.commandSocket.connect(self.commandSocketAdress)
        self.commandSocket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        self.commandSocket.setsockopt(zmq.LINGER,0)

    def createGetCurrentStateTableSocket(self):
        if self.socketGetCurrentStateTable:
            #reset
            self.socketGetCurrentStateTable.close()
        self.socketGetCurrentStateTable = self.context.socket(zmq.DEALER)
        self.socketGetCurrentStateTable.connect(self.getCurrentStateTableAddress)
        self.socketGetCurrentStateTable.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
        self.socketGetCurrentStateTable.setsockopt(zmq.LINGER,0)

    def sendCommand(self,command):
        """send command to pca return True on Success"""
        self.commandSocket.send(command)
        try:
            r = self.commandSocket.recv()
        except zmq.Again:
            self.handleDisconnection()
            return False
        if r != ECSCodes.ok:
            logQueue.put("received error for sending command: " + command)
            return False
        return True

    def handleDisconnection(self):
        print("timeout PCA")
        self.logQueue.put("PCA Connection Lost")
        #reset commandSocket
        self.createCommandSocket()
        self.PCAConnection = False
        while not self.PCAConnection:
            self.getStateSnapshot()


    def receive_status(self,socket):
        try:
            id, sequence, state = socket.recv_multipart()
        except zmq.Again:
            return None
        except:
            print ("error while receiving")
            return
        if id != b"":
            id = struct.unpack("!i",id)[0]
        else:
            id = None
        sequence = struct.unpack("!i",sequence)[0]
        if state != b"":
            state = state.decode()
        else:
            state = None
        return [id,sequence,state]

    def getStateSnapshot(self):
        """get current stateMap from pca returns True on success"""
        sequence = 0
        self.socketGetCurrentStateTable.send(ECSCodes.hello)
        while True:
            ret = self.receive_status(self.socketGetCurrentStateTable)
            if ret == None:
                print ("error while getting snapshot")
                #reset Socket to clear the buffer, PCA only needs one hello
                self.createGetCurrentStateTableSocket()
                return False
            id, sequence, state = ret
            print (id,sequence,state)
            if id != None:
                self.stateMap[id] = (sequence, state)
            #id should be None in final message
            else:
                self.PCAConnection = True
                self.logQueue.put("PCA Connection online")
                return True

    def waitForUpdates(self):
        while True:
            try:
                m = self.socketSubscription.recv_multipart()
            except:

                #the timeout only exits so that the recv doesn't deadlock
                continue
            if len(m) != 3:
                print (m)
            else:
                id,sequence,state = m
            #id, sequence, state = socketSubscription.recv_multipart()
            id = struct.unpack("!i",id)[0]
            sequence = struct.unpack("!i",sequence)[0]
            print("received update",id, sequence, state)
            self.stateMap[id] = (sequence, state.decode())

    def waitForLogUpdates(self):
        while True:
            m = self.socketSubLog.recv().decode()
            self.logQueue.put(m)



pca = PCAHandler()
#views
def update(request):
    newlogs = []
    while not pca.logQueue.empty():
        newlogs.append(pca.logQueue.get())
    return render(request, 'GUI/states.html', {'stateMap': pca.stateMap, 'logs': newlogs })

"""
def logUpdate(request):
    newlogs = []
    while not pca.logQueue.empty():
        newlogs.append(pca.logQueue.get())
    return render(request, 'GUI/logs.html', {'logs': newlogs})
"""

def ready(request):
    print ("sending ready")
    pca.commandSocketQueue.put(b"ready")
    return HttpResponse(status=200)

def start(request):
    print ("sending start")
    pca.commandSocketQueue.put(b"start")
    return HttpResponse(status=200)

def shutdown(request):
    print ("sending shutdown")
    pca.commandSocketQueue.put(b"shutdown")
    return HttpResponse(status=200)

def stop(request):
    print ("sending stop")
    pca.commandSocketQueue.put(b"stop")
    return HttpResponse(status=200)

def index(request):
    print (pca.stateMap)
    return render(request, "GUI/monitor.html",{'stateMap': pca.stateMap})
