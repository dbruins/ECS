from django.contrib import admin
from guardian.admin import GuardedModelAdmin

from .models import pcaModel, ecsModel

#registers ecs and pca permissions in djnago admin page
class PCAAdminPCA(GuardedModelAdmin):
    prepopulated_fields = {"id": ("id",)}
    exclude = ('has_pca_control',)
    ordering = ('-id',)


class PCAAdminECS(GuardedModelAdmin):
    prepopulated_fields = {"id": ("id",)}
    exclude = ('has_ecs_control',)
    ordering = ('-id',)

admin.site.register(pcaModel,PCAAdminPCA)
admin.site.register(ecsModel,PCAAdminECS)
