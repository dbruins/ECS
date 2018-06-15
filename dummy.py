import zmq
import sys
import time
import thread

if (len(sys.argv) > 1):
  address = sys.argv[1]
else:
  address = "5555"

context = zmq.Context()
socketReceiver = context.socket(zmq.REP)
socketReceiver.bind(("tcp://*:" + address))

socketSender = context.socket(zmq.REQ)
socketSender.connect("tcp://localhost:%i" % 5553)


def waitForCommand():
  while True:
      m = socketReceiver.recv()
      print m
      #m = raw_input()
      socketReceiver.send("OK")
      
      
thread.start_new_thread(waitForCommand,())



id = int(address) - 5554
print id
while True:  
  x = raw_input()

  socketSender.send("%i %s" % (id, x))
  #socketSender.send(x)
  print socketSender.recv()
  

