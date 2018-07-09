from channels.generic.websocket import WebsocketConsumer
from asgiref.sync import async_to_sync
import json

class updateConsumer(WebsocketConsumer):
    group_name = "update"
    def connect(self):
        # Join update group
        async_to_sync(self.channel_layer.group_add)(
            self.group_name,
            self.channel_name
        )

        self.accept()

    def disconnect(self, close_code):
        pass

    def receive(self, text_data):
        """receive message from websocket"""
        #currently not used (could possibly be used instead of ajax requests for commands)
        print (text_data)
        text_data_json = json.loads(text_data)
        message = text_data_json["message"]

    def update(self,event):
        self.send(text_data=json.dumps({
            "type": "state",
            "message": event["text"]
        }))

    def logUpdate(self,event):
        self.send(text_data=json.dumps({
            "type": "log",
            "message": event["logText"]
        }))
