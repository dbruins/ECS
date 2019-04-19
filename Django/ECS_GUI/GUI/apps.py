from django.apps import AppConfig
import sys

class GuiConfig(AppConfig):
    name = 'GUI'
    def ready(self):
        #called as soon as App is ready
        pass
        #self.test = "ready"
