#!/usr/bin/python3
import asyncio
import datetime
import websockets
import threading
import re
import json
from multiprocessing import Queue

class WebSocket:

    def __init__(self,address,port):
        self.openConnections = {}
        self.webSocketLocks = {}
        asyncio.set_event_loop(asyncio.new_event_loop())
        start_server = websockets.serve(self.accept, address,port)

        asyncio.get_event_loop().run_until_complete(start_server)
        f= asyncio.get_event_loop().run_forever
        t=threading.Thread(target=f)
        t.start()

        self.updateQueue = Queue()

    def addPCA(self,pcaId):
        self.openConnections[pcaId]=set()

    def removePCA(self,pcaId):
        if pcaId in self.openConnections:
            del self.openConnections[pcaId]

    def addWebSocket(self,webSocket,group,user):
        self.webSocketLocks[webSocket]=threading.Lock()
        self.openConnections[group].add(webSocket)
        self.openConnections[user].add(webSocket)

    def removeWebSocket(self,webSocket,group,user):
        self.openConnections[group].remove(webSocket)
        self.openConnections[user].remove(webSocket)
        if self.webSocketLocks[webSocket].locked():
            self.webSocketLocks[webSocket].release()
        del self.webSocketLocks[webSocket]


    def sendUpdate(self,update,pcaId):
        l = asyncio.new_event_loop()
        l.run_until_complete(self.__async_sendUpdate(update,pcaId))

    @asyncio.coroutine
    async def __my_send(self,user,message,lock):
        try:
            lock.acquire()
            await user.send(json.dumps(message))
        finally:
            lock.release()

    async def __async_sendUpdate(self,update,pcaId):
        message = {
            "type": "state",
            "message": update,
            "origin" : pcaId,
        }
        if pcaId in self.openConnections and self.openConnections[pcaId]:
            #await asyncio.wait([user.send(json.dumps(message)) for user in self.openConnections[pcaId]])
            await asyncio.wait([self.__my_send(user,message,self.webSocketLocks[user]) for user in self.openConnections[pcaId]])

        if "ecs" in self.openConnections and self.openConnections["ecs"]:
            await asyncio.wait([self.__my_send(user,message,self.webSocketLocks[user]) for user in self.openConnections["ecs"]])

    def sendLogUpdate(self,update,pcaId):
        l = asyncio.new_event_loop()
        l.run_until_complete(self.__async_sendLogUpdate(update,pcaId))

    async def __async_sendLogUpdate(self,logmessage,pcaId):
        message = {
            "type": "log",
            "message": logmessage,
            "origin": pcaId
        }
        if pcaId in self.openConnections and self.openConnections[pcaId]:
            await asyncio.wait([self.__my_send(user,message,self.webSocketLocks[user]) for user in self.openConnections[pcaId]])

    def permissionTimeout(self,user):
        l = asyncio.new_event_loop()
        l.run_until_complete(self.__async_permissionTimeout(user))

    async def __async_permissionTimeout(self,userGroup):
        message = {
            "type": "permissionTimeout",
        }
        if userGroup in self.openConnections and self.openConnections[userGroup]:
            await asyncio.wait([user.send(json.dumps(message)) for user in self.openConnections[userGroup]])


    async def accept(self,websocket, path):
        group,user = re.search("/(\w+)/(\w+)",path).groups()
        print(path,group,user)
        if group not in self.openConnections:
            self.openConnections[group]=set()
        if user not in self.openConnections:
            self.openConnections[user]=set()
        self.addWebSocket(websocket,group,user)
        try:
            async for message in websocket:
                pass
                #we don't expect messages just keep the connection open until it closes
        finally:
            self.removeWebSocket(websocket,group,user)
