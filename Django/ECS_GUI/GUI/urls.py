from django.urls import path
from django.conf.urls import include

from . import views

app_name='GUI'
#todo maybe one Path for all requests?
urlpatterns = [
    path('accounts/', include('django.contrib.auth.urls')),
    path('', views.index.as_view(), name='index'),
    path('pca/<str:pcaId>', views.pcaView.as_view(), name='pca'),
    path('createPartition/', views.create_pca.as_view(), name='create_pca'),
    path('deletePartition/',views.delete_pca.as_view(), name='delete_pca'),
    path('createDetector/', views.create_detector.as_view(), name='create_detector'),
    path('deleteDetectors', views.deleteDetector.as_view(), name='delete_detectors'),
    path('editPartition', views.input_edit_pca, name='input_edit_pca'),
    path('editPartition/edit', views.edit_pca, name='edit_pca'),
    path('moveDetectors', views.moveDetectors.as_view(), name="move_detectors"),
    path('ready/<str:pcaId>', views.ready.as_view(), name='ready'),
    path('start/<str:pcaId>', views.start.as_view(), name='start'),
    path('stop/<str:pcaId>', views.stop.as_view(), name='stop'),
    path('abort/<str:pcaId>', views.abort.as_view(), name='abort'),
    path('shutdown/<str:pcaId>', views.shutdown.as_view(), name='shutdown'),
    path('setActive/<str:pcaId>',views.setActive.as_view(), name='setActive' ),
    path('setInactive/<str:pcaId>',views.setInactive.as_view(), name='setInactive' ),
    path('getDetectorListForPca/',views.getDetectorListForPCA,name='getDetectorListForPCA'),
    path('take_control/<str:pcaId>', views.takeControl, name='take_control'),
    path('giveup_control/<str:pcaId>', views.giveUpControl, name='giveup_control'),
]
