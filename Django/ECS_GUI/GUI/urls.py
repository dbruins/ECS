from django.urls import path
from django.conf.urls import include

from . import views

app_name='GUI'
#todo maybe one Path for all requests?
urlpatterns = [
    path('accounts/', include('django.contrib.auth.urls')),
    path('', views.index, name='index'),
    path('take_control/<str:pcaId>', views.takeControl, name='take_control'),
    path('giveup_control/<str:pcaId>', views.giveUpControl, name='giveup_control'),
    path('pca/<str:pcaId>', views.pca, name='pca'),
    path('createPartition/', views.input_create_pca, name='input_create_pca'),
    path('createPartition/create', views.create_pca, name='create_pca'),
    path('createDetector/', views.input_create_detector, name='input_create_detector'),
    path('createDetector/create', views.create_detector, name='create_detector'),
    path('editPartition', views.input_edit_pca, name='input_edit_pca'),
    path('editPartition/edit', views.edit_pca, name='edit_pca'),
    path('moveDetectors', views.input_MoveDetectors, name="input_move_detectors"),
    path('moveDetectors/move', views.moveDetectors, name="move_detectors"),
    path('ready/<str:pcaId>', views.ready, name='ready'),
    path('start/<str:pcaId>', views.start, name='start'),
    path('stop/<str:pcaId>', views.stop, name='stop'),
    path('shutdown/<str:pcaId>', views.shutdown, name='shutdown'),
    path('setActive/<str:pcaId>',views.setActive, name='setActive' ),
    path('setInactive/<str:pcaId>',views.setInactive, name='setInactive' ),
    path('getDetectorListForPca/',views.getDetectorListForPCA,name='getDetectorListForPCA'),
    path('about/', views.AboutView.as_view()),
]
