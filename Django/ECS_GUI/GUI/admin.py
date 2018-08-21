from django.contrib import admin

from .models import Question, Choice, pcaModel
admin.site.register(Question)
admin.site.register(Choice)
admin.site.register(pcaModel)
