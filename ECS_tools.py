import threading
import copy
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

def send_status(socket,id,sequence,state):
    """method for sending a status update on a specified socket"""
    #if None send empty byte String
    id_b=b""
    if id != None:
        id_b = key.encode()

    #integers need to be packed
    sequence_s = struct.pack("!i",sequence)
    #python strings need to be encoded into binary strings
    state_b = b""
    if state != None:
        state_b = state.encode()
    socket.send_multipart([id_b,sequence_s,state_b])

def receive_status(socket,pcaid):
    try:
        id, sequence, state = socket.recv_multipart()
    except zmq.Again:
        print ("timeout receiving status for %s" % pcaid)
        return None
    except Exception as e:
        print ("error receiving status for %s: %s" % (pcaid,str(e)))
        return None
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

def getStateSnapshot(stateMap,pcaid,address,port):
    """get snapshot of State Table from a PCA this needs to happen to regard a PCA as connected"""
    socketGetCurrentStateTable = None
    socketGetCurrentStateTable = self.resetSocket(socketGetCurrentStateTable,address,port,zmq.DEALER)
    socketGetCurrentStateTable.setsockopt(zmq.RCVTIMEO, self.receive_timeout)
    socketGetCurrentStateTable.send(ECSCodes.hello)
    while True:
        ret = self.receive_status(socketGetCurrentStateTable,pcaid)
        if(ret == None):
            socketGetCurrentStateTable.close()
            return False

        id, sequence, state = ret
        print (id,sequence,state)
        if id != None:
            self.stateMap[id] = (sequence, state)
        #id should be None in final message
        else:
            #todo this is kind of stupid
            #self.connectedPartitions[pcaid] = pcaid
            socketGetCurrentStateTable.close()
            return True

#todo same as in Detector
def getConfsectionForType(type):
        confSection = {
            "DetectorA" : "DETECTOR_A",
            "DetectorB" : "DETECTOR_B",
        }
        return confSection[type]
