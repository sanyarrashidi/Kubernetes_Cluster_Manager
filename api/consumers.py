import json

from channels.generic.websocket import AsyncWebsocketConsumer

GROUP_NAME = "cluster-status"


class StatusConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.channel_layer.group_add(GROUP_NAME, self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(GROUP_NAME, self.channel_name)

    async def status_update(self, event):
        await self.send(text_data=json.dumps(event["data"]))
