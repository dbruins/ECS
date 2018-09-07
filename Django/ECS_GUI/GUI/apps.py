from django.apps import AppConfig
import zc.lockfile
import sys

class GuiConfig(AppConfig):
    name = 'GUI'
    def ready(self):

        self.test = "ready"
        print(sys.argv)
