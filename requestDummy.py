
import zmq
import sys
import time
import _thread
import struct

context = zmq.Context()
socketSender = context.socket(zmq.REQ)
socketSender.connect("tcp://localhost:%i" % 5553)

while True:
    print("enter id:")
    id = input()
    socketSender.send(id.encode())
    ret = socketSender.recv()
    print (ret.decode())
