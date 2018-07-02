from django.shortcuts import get_object_or_404,render
from django.http import HttpResponse, HttpResponseRedirect
from GUI.models import Question, Choice
from django.template import loader
from django.urls import reverse
import zmq
import threading
import struct
from multiprocessing import Queue

logQueue=Queue()
stateMap = {}

context = zmq.Context()
socketSubscription = context.socket(zmq.SUB)
socketSubscription.connect("tcp://localhost:%i" % 5555)
#subscribe to everything
socketSubscription.setsockopt(zmq.SUBSCRIBE, b'')

socketGetCurrentStateTable = context.socket(zmq.DEALER)
socketGetCurrentStateTable.connect("tcp://localhost:%i" % 5557)

socketSubLog = context.socket(zmq.SUB)
socketSubLog.connect("tcp://localhost:%i" % 5551)
#subscribe to everything
socketSubLog.setsockopt(zmq.SUBSCRIBE, b'')

commandSocket = context.socket(zmq.REQ)
commandSocket.connect("tcp://localhost:%i" % 5552)

def receive_status(socket):
    try:
        id, sequence, state = socket.recv_multipart()
    except:
        print ("receive error")
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

def waitForUpdates():
    #get current state
    sequence = 0
    socketGetCurrentStateTable.send_string("HI")
    while True:
        id, sequence, state = receive_status(socketGetCurrentStateTable)
        print (id,sequence,state)
        if id != None:
            stateMap[id] = (sequence, state)
        #id should be None in final message
        else:
            break

    #watch subscription for further updates
    while True:
        m = socketSubscription.recv_multipart()
        if len(m) != 3:
            print (m)
        else:
            id = m[0]
            sequence = m[1]
            state = m[2]
        #id, sequence, state = socketSubscription.recv_multipart()
        id = struct.unpack("!i",id)[0]
        sequence = struct.unpack("!i",sequence)[0]
        print("received update",id, sequence, state)
        stateMap[id] = (sequence, state.decode())

def waitForLogUpdates():
    while True:
        m = socketSubLog.recv().decode()
        logQueue.put(m)

#start thread only once
started = False
if started == False:
    started = True
    t = threading.Thread(name="updater", target=waitForUpdates)
    t.start()
    t = threading.Thread(name="logUpdater", target=waitForLogUpdates)
    t.start()

def update(request):
    return render(request, 'GUI/states.html', {'stateMap': stateMap})

def logUpdate(request):
    newlogs = []
    while not logQueue.empty():
        newlogs.append(logQueue.get())
    return render(request, 'GUI/logs.html', {'logs': newlogs})

def ready(request):
    print ("sending ready")
    commandSocket.send(b"ready")
    r = commandSocket.recv().decode()
    if r == "OK":
        return render(request, "GUI/monitor.html",{'stateMap': stateMap ,'error':False})
    else:
        return render(request, "GUI/monitor.html",{'stateMap': stateMap, 'error':True})

def start(request):
    commandSocket.send(b"start")
    r = commandSocket.recv().decode()
    if r == "OK":
        return render(request, "GUI/monitor.html",{'stateMap': stateMap ,'error':False})
    else:
        return render(request, "GUI/monitor.html",{'stateMap': stateMap, 'error':True})

def shutdown(request):
    commandSocket.send(b"shutdown")
    r = commandSocket.recv().decode()
    if r == "OK":
        return render(request, "GUI/monitor.html",{'stateMap': stateMap ,'error':False})
    else:
        return render(request, "GUI/monitor.html",{'stateMap': stateMap, 'error':True})

def stop(request):
    commandSocket.send(b"stop")
    r = commandSocket.recv().decode()
    if r == "OK":
        return render(request, "GUI/monitor.html",{'stateMap': stateMap ,'error':False})
    else:
        return render(request, "GUI/monitor.html",{'stateMap': stateMap, 'error':True})

def index(request):
    print (stateMap)
    return render(request, "GUI/monitor.html",{'stateMap': stateMap})
