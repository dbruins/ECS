from django.urls import path
from django.conf.urls import include

from . import views

app_name='GUI'
urlpatterns = [
    path('accounts/', include('django.contrib.auth.urls')),
    path('', views.index.as_view(), name='index'),
    path('pca/<str:pcaId>', views.pcaView.as_view(), name='pca'),
    path('createPartition/', views.create_pca.as_view(), name='create_pca'),
    path('deletePartition/',views.delete_pca.as_view(), name='delete_pca'),
    path('createDetector/', views.create_detector.as_view(), name='create_detector'),
    path('deleteDetectors', views.deleteDetector.as_view(), name='delete_detectors'),
    path('moveDetectors', views.moveDetectors.as_view(), name="move_detectors"),
    path('ready/<str:pcaId>', views.ready.as_view(), name='ready'),
    path('start/<str:pcaId>', views.start.as_view(), name='start'),
    path('stop/<str:pcaId>', views.stop.as_view(), name='stop'),
    path('abort/<str:pcaId>', views.abort.as_view(), name='abort'),
    path('reset/<str:pcaId>', views.reset.as_view(), name='reset'),
    path('getDetectorListForPca/',views.getDetectorListForPCA,name='getDetectorListForPCA'),
    path('currentTableAndLogRequest/<str:pcaId>',views.currentTableAndLogRequest,name='currentTableAndLogRequest'),
    path('take_pca_control/<str:pcaId>/<str:targetPage>', views.takePCAControl, name='take_pca_control'),
    path('giveup_pca_control/<str:pcaId>/<str:targetPage>', views.giveUpPCAControl, name='giveup_pca_control'),
    path('take_ecs_control/<str:targetPage>', views.takeECSControl, name='take_ecs_control'),
    path('giveup_ecs_control/<str:targetPage>', views.giveUpECSControl, name='giveup_ecs_control'),
    path('configureTagModal/<str:pcaId>', views.configTagModalView.as_view(), name="configureTagModal"),
    path('getConfigsForTag/', views.getConfigsForTag, name="getConfigsForTag"),
    path('getConfigsForSystem/', views.getConfigsForSystem, name="getConfigsForSystem"),
    path('editConfiguration/', views.editConfiguration.as_view(), name="editConfiguration"),
    path('editConfigurationTag/', views.editConfigurationTag.as_view(), name="editConfigurationTag"),
    path('clients/',views.clientPage.as_view(), name='clients'),
]
