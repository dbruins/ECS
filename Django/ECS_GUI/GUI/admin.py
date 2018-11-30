from django.contrib import admin
from guardian.admin import GuardedModelAdmin

from .models import pcaModel


class PCAAdmin(GuardedModelAdmin):
    prepopulated_fields = {"id": ("id",)}
    exclude = ('has_control',)
    ordering = ('-id',)

admin.site.register(pcaModel,PCAAdmin)
