from django.apps import AppConfig

class GuiConfig(AppConfig):
    name = 'GUI'
    def ready(self):
        print("ready")
