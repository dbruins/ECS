from channels.generic.websocket import WebsocketConsumer
from channels.exceptions import StopConsumer
from asgiref.sync import async_to_sync
import json
#  >>>code for django channels websockets (not used anymore)<<<

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
        raise StopConsumer()

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
        #print("Consumer: ",json.loads(event["text"])["id"],json.loads(event["text"])["state"])
        message = {
            "type": "state",
            "message": event["text"]
        }
        if "origin" in event:
            message["origin"] = event["origin"]
        self.send(text_data=json.dumps(message))

    def logUpdate(self,event):
        message = {
            "type": "log",
            "message": event["text"]
        }
        if "origin" in event:
            message["origin"] = event["origin"]
        self.send(text_data=json.dumps(message))

    def permissionTimeout(self,event):
        message = {
            "type": "permissionTimeout",
        }
        self.send(text_data=json.dumps(message))
