#!/usr/bin/python3
import asyncio
import datetime
import websockets
import threading
import re
import json
import ssl
from django.contrib.sessions.models import Session
from django.contrib.auth.models import User
from django.conf import settings

class WebSocket:
    """websockets for the web UI"""
    def __init__(self,address,port):
        self.openConnections = {}
        self.webSocketLocks = {}

        if settings.ENCRYPT_WEBSOCKET:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ssl_context.load_cert_chain(settings.SSL_CERT_FILE,settings.SSL_PEM_FILE)
        else:
            ssl_context = None

        asyncio.set_event_loop(asyncio.new_event_loop())
        #each time a new client connects, the function accept is called
        start_server = websockets.serve(self.accept,address,port, ssl=ssl_context)

        #start the server in a new thread
        asyncio.get_event_loop().run_until_complete(start_server)
        f= asyncio.get_event_loop().run_forever
        t=threading.Thread(target=f)
        t.start()

    def addPCA(self,pcaId):
        """create new connections entry for a new pca"""
        self.openConnections[pcaId]=set()

    def removePCA(self,pcaId):
        """remove open connections for a pca"""
        if pcaId in self.openConnections:
            del self.openConnections[pcaId]

    def addUser(self,username,token):
        """adds a user with a authentification token"""
        self.user_tokens[username] = token

    def removeUser(self,username):
        """removes a user"""
        del self.user_tokens[username]

    def addWebSocket(self,webSocket,group,user):
        """add websocket to user and group sets"""
        self.webSocketLocks[webSocket]=threading.Lock()
        self.openConnections[group].add(webSocket)
        self.openConnections[user].add(webSocket)

    def removeWebSocket(self,webSocket,group,user):
        """remove websocket from groups"""
        if group in self.openConnections and webSocket in self.openConnections[group]:
            self.openConnections[group].remove(webSocket)
        if user in self.openConnections and webSocket in self.openConnections[user]:
            self.openConnections[user].remove(webSocket)
        if webSocket in self.webSocketLocks:
            if self.webSocketLocks[webSocket].locked():
                self.webSocketLocks[webSocket].release()
            del self.webSocketLocks[webSocket]


    def sendUpdate(self,update,pcaId):
        """send update to a websocket pca group"""
        l = asyncio.new_event_loop()
        l.run_until_complete(self.__async_sendUpdate(update,pcaId))

    @asyncio.coroutine
    async def __async_send(self,user,message,lock):
        try:
            #websockets are not thread safe, only one thread at a time can use them
            lock.acquire()
            await user.send(json.dumps(message))
        finally:
            lock.release()

    async def __async_sendUpdate(self,update,pcaId):
        """send update to a websocket pca group"""
        message = {
            "type": "state",
            "message": update,
            "origin" : pcaId,
        }
        if pcaId in self.openConnections and self.openConnections[pcaId]:
            #await asyncio.wait([user.send(json.dumps(message)) for user in self.openConnections[pcaId]])
            await asyncio.wait([self.__async_send(user,message,self.webSocketLocks[user]) for user in self.openConnections[pcaId]])

        if "ecs" in self.openConnections and self.openConnections["ecs"]:
            await asyncio.wait([self.__async_send(user,message,self.webSocketLocks[user]) for user in self.openConnections["ecs"]])

    def sendLogUpdate(self,update,pcaId):
        """send logupdate to a pca group"""
        l = asyncio.new_event_loop()
        l.run_until_complete(self.__async_sendLogUpdate(update,pcaId))

    async def __async_sendLogUpdate(self,logmessage,pcaId):
        """send logupdate to a pca group"""
        message = {
            "type": "log",
            "message": logmessage,
            "origin": pcaId
        }
        if pcaId in self.openConnections and self.openConnections[pcaId]:
            await asyncio.wait([self.__async_send(user,message,self.webSocketLocks[user]) for user in self.openConnections[pcaId]])

    def permissionTimeout(self,user):
        """send iformation about a permission timeout to a user"""
        l = asyncio.new_event_loop()
        l.run_until_complete(self.__async_permissionTimeout(user))

    async def __async_permissionTimeout(self,userGroup):
        """send iformation about a permission timeout to a user"""
        message = {
            "type": "permissionTimeout",
        }
        if userGroup in self.openConnections and self.openConnections[userGroup]:
            await asyncio.wait([user.send(json.dumps(message)) for user in self.openConnections[userGroup]])


    async def accept(self,websocket, path):
        """accepts a new connection"""
        res = re.search("/(\w+)/(\w+)",path)
        if res:
            group,username = res.groups()
        else:
            return
        try:
            try:
                user = User.objects.get(username=username)
            except User.DoesNotExist:
                print("unknown user %s tried to connect to websocket for %s" % (username,group))
                return
            #get user token from django database
            if hasattr(user, 'logged_in_user'):
                session_key_for_user = user.logged_in_user.session_key
            else:
                print("not authenticated user %s tried to connect to websocket for %s" % (username,group))
                return
            #wait for client to send authentification token
            session_key_message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
            #check if session keys match
            if session_key_message != session_key_for_user:
                print("user %s tried to connect to websocket for %s with invalid sessionId" % (username,group))
                #reject
                return
            else:
                #accept
                if group not in self.openConnections:
                    self.openConnections[group]=set()
                if username not in self.openConnections:
                    self.openConnections[username]=set()
                self.addWebSocket(websocket,group,username)
            async for message in websocket:
                pass
                #we don't expect messages just keep the connection open until it closes
        except asyncio.TimeoutError:
            print("authentication timeout for user %s" % username)
        finally:
            self.removeWebSocket(websocket,group,username)
