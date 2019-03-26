import zc.lockfile
import zmq
import subprocess
import os
import errno
from ECSCodes import ECSCodes
codes = ECSCodes()

class BaseController:
    def __init__(self,id,socketPort):
        self.MyId = id

        #create lock
        try:
            self.lock = zc.lockfile.LockFile('/tmp/lock'+self.MyId, content_template='{pid}')
        except zc.lockfile.LockError:
            print("another Process is already Running "+self.MyId)
            exit(1)

        self.context = zmq.Context()
        self.context.setsockopt(zmq.LINGER,0)
        self.commandSocket = self.context.socket(zmq.REP)
        self.commandSocket.bind("tcp://*:%i" % socketPort)

        self.abort = False
        self.scriptProcess = None

    def executeScript(self,scriptname):
        self.scriptProcess = subprocess.Popen(["exec ./"+scriptname], shell=True)
        ret = self.scriptProcess.wait()
        if self.abort:
            return False
        if ret:
            return False
        else:
            return True

    def waitForPipeMessages(self):
        try:
            os.mkfifo("pipe"+self.MyId)
        except OSError as oe:
            if oe.errno != errno.EEXIST:
                raise
        while True:
            with open("pipe"+self.MyId) as pipe:
                message = pipe.read().replace('\n', '')
                if len(message) == 0:
                    #pipe closed
                    break
                elif message == "closeThread":
                    if os.path.exists("pipe"+self.MyId):
                        os.remove("pipe"+self.MyId)
                    return
                self.handleSystemMessage(message)

    def endPipeThread(self):
        pipe = open("pipe"+self.MyId, 'w')
        pipe.write("closeThread")


    def waitForCommands(self):
        """wait for command on the command socket"""
        while True:
            try:
                message = self.commandSocket.recv_multipart()
                if self.handleCommand(message):
                    continue
                self.commandSocket.send(codes.unknownCommand)
            except zmq.error.ContextTerminated:
                self.commandSocket.close()
                break

    def handleCommand(self,message):
        pass

    def handleSystemMessage(self,message):
        pass
