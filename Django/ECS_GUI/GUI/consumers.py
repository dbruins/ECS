from channels.generic.websocket import WebsocketConsumer
from asgiref.sync import async_to_sync
import json

class updateConsumer(WebsocketConsumer):
    def connect(self):
        self.pcaId = self.scope['url_route']['kwargs']['pca_id']
        user = self.scope["user"]
        self.group_name = self.pcaId

        if not user.is_authenticated:
            self.close()
        self.user = user.username

        # Join pca/ecs group
        async_to_sync(self.channel_layer.group_add)(
            self.group_name,
            self.channel_name
        )
        #User Specific group
        async_to_sync(self.channel_layer.group_add)(
            self.user,
            self.channel_name
        )

        self.accept()

    def disconnect(self, close_code):
        pass

    def receive(self, text_data):
        """receive message from websocket"""
        pcaId = text_data
        async_to_sync(self.channel_layer.send)(
            "ecs",
            {
                'pcaId': pcaId,
                'user': self.user
            }
        )

    def update(self,event):
        message = {
            "type": "state",
            "message": event["text"]
        }
        if "origin" in event:
            message["origin"] = event["origin"]
        self.send(text_data=json.dumps(message))

    def stateTable(self,event):
        message = {
            "type": "table_"+event["id"],
            "message": event["text"]
        }
        self.send(text_data=json.dumps(message))

    def bufferedLog(self,event):
        message = {
            "type": "bufferedLog_"+event['id'],
            "message": event["text"]
        }
        self.send(text_data=json.dumps(message))

    def logUpdate(self,event):
        message = {
            "type": "log",
            "message": event["text"]
        }
        if "origin" in event:
            message["origin"] = event["origin"]
        self.send(text_data=json.dumps(message))
