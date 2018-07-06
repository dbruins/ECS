import zmq
import sys
import time
import _thread
import struct
from random import randint
import ECSCodes
import configparser
config = configparser.ConfigParser()
config.read("init.cfg")

stateMap = {}

if (len(sys.argv) > 1):
    address = sys.argv[1]
else:
    address = "5558"

ports = config["ZMQPorts"]
context = zmq.Context()
socketReceiver = context.socket(zmq.REP)
socketReceiver.bind(("tcp://*:" + address))

socketSubscription = context.socket(zmq.SUB)
socketSubscription.connect("tcp://localhost:%s" % ports["statePublish"])
#subscribe to everything
socketSubscription.setsockopt(zmq.SUBSCRIBE, b'')

socketPushUpdate = context.socket(zmq.PUSH)
socketPushUpdate.connect("tcp://localhost:%s" % ports["pullUpdates"])

socketGetCurrentStateTable = context.socket(zmq.DEALER)
socketGetCurrentStateTable.connect("tcp://localhost:%s" % ports["serveCurrentStatus"])


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
    # Get state snapshot
    sequence = 0
    socketGetCurrentStateTable.send(ECSCodes.hello)
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
            state = m[2].decode()
        #id, sequence, state = socketSubscription.recv_multipart()
        id = struct.unpack("!i",id)[0]
        sequence = struct.unpack("!i",sequence)[0]
        print("received update",id, sequence, state)
        stateMap[id] = (sequence, state)

def waitForCommand():
    while True:
        m = socketReceiver.recv()
        print (m)
        time.sleep(randint(2,6))
        #m = input()
        socketReceiver.send(ECSCodes.ok)


_thread.start_new_thread(waitForCommand,())
_thread.start_new_thread(waitForUpdates,())


id = int(address) - 5557
print (id)
while True:
    try:
        x = input()
    except EOFError:
        continue
    socketPushUpdate.send_string("%i %s" % (id, x))
