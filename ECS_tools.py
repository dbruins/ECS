import threading
import copy
import zmq
import ECSCodes
import struct
class MapWrapper:
    """thread safe handling of Map"""
    def __init__(self):
        self.map = {}
        self.semaphore = threading.Semaphore()

    def __iter__(self):
        return self.copy().values().__iter__()

    def __getitem__(self, key):
        """get value for key returns None if key doesn't exist"""
        self.semaphore.acquire()
        if key in self.map:
            ret = self.map[key]
        else:
            ret = None
        self.semaphore.release()
        return ret

    def __delitem__(self,key):
        self.semaphore.acquire()
        if key in self.map:
            del self.map[key]
        self.semaphore.release()


    def __setitem__(self,key,value):
        self.semaphore.acquire()
        self.map[key] = value
        self.semaphore.release()

    def __str__(self):
        self.semaphore.acquire()
        ret = self.map.__str__()
        self.semaphore.release()
        return ret

    def copy(self):
        """returns a deepcopy off all items for iteration"""
        #create copy of statusMap so loop dosn't crash if there are changes on statusMap during the loop
        #probably not the best solution
        self.semaphore.acquire()
        mapCopy = copy.deepcopy(self.map)
        self.semaphore.release()
        return mapCopy

    def __contains__(self, key):
        self.semaphore.acquire()
        ret = key in self.map
        self.semaphore.release()
        return ret

    def size(self):
        self.semaphore.acquire()
        ret = len(self.map)
        self.semaphore.release()
        return ret

    def delMany(self,items):
        """deletes a given list of ids"""
        self.semaphore.acquire()
        for i in items:
            if i in self.map:
                del self.map[i]
        self.semaphore.release()


    def reset(self):
        self.semaphore.acquire()
        self.map = {}
        self.semaphore.release()

def resetSocket(self,socket,address,port,type):
    """resets a socket with address and zmq Type; if socket is None a new socket will be created"""
    if socket != None:
        socket.close()
    socket = self.zmqContext.socket(type)
    socket.connect("tcp://%s:%s" % (address,port))
    if type == zmq.REQ:
        socket.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
    socket.setsockopt(zmq.LINGER,0)
    return socket

def intToBytes(x):
    """convert an integer to a byte array"""
    return x.to_bytes(((x.bit_length() -1) // 8 ) +1,'big')

def intFromBytes(x):
    """convert a byte_array to an integer"""
    return int.from_bytes(x,'big')

def send_status(socket,id,sequence,state):
    """method for sending a status update on a specified socket"""
    if isinstance(id,str):
        id = id.encode()

    sequence = intToBytes(sequence)

    #python strings need to be encoded into binary strings
    if isinstance(state,str):
        state = state.encode()
    socket.send_multipart([id,sequence,state])

def receive_status(socket,pcaid=None):
    """receive status from PCA retuns None on error"""
    if pcaid:
        errorString = " receiving status for %s" % pcaid
    else:
        errorString = " receiving status"
    try:
        id, sequence, state = socket.recv_multipart()
    except zmq.Again:
        print ("timeout"+errorString)
        return None
    except Exception as e:
        print ("error"+errorString)
        return None
    if id != ECSCodes.done:
        id = id.decode()

    sequence = intFromBytes(sequence)
    if state != b"":
        state = state.decode()
    else:
        state = None
    return [id,sequence,state]

def getStateSnapshot(stateMap,address,port,timeout=2000,pcaid=None):
    """get snapshot of State Table from a PCA this needs to happen to regard a PCA as connected"""
    context = zmq.Context()
    socketGetCurrentStateTable = context.socket(zmq.DEALER)
    socketGetCurrentStateTable.setsockopt(zmq.RCVTIMEO, timeout)
    socketGetCurrentStateTable.setsockopt(zmq.LINGER,0)
    socketGetCurrentStateTable.connect("tcp://%s:%s" % (address,port))
    socketGetCurrentStateTable.send(ECSCodes.hello)
    while True:
        ret = receive_status(socketGetCurrentStateTable,pcaid)
        #receive_status returns None if something went wrong
        if(ret == None):
            socketGetCurrentStateTable.close()
            return False
        id, sequence, state = ret
        if id != ECSCodes.done:
            print (id,sequence,state)
            if id in stateMap:
                if stateMap[id][0] < sequence:
                    stateMap[id] = (sequence, state)
            else:
                stateMap[id] = (sequence, state)
        #id should be ECSCodes.done in final message
        else:
            print ("done")
            socketGetCurrentStateTable.close()
            return True

#todo same as in Detector
def getConfsectionForType(type):
        confSection = {
            "DetectorA" : "DETECTOR_A",
            "DetectorB" : "DETECTOR_B",
        }
        return confSection[type]
