from django.urls import path

from . import views

app_name='GUI'
#todo maybe one Path for all requests?
urlpatterns = [
    path('', views.index, name='index'),
    path('pca/<str:pcaId>', views.pca, name='pca'),
    path('createPartition/', views.input_create_pca, name='input_create_pca'),
    path('createPartition/create', views.create_pca, name='create_pca'),
    path('createDetector/', views.input_create_detector, name='input_create_detector'),
    path('createDetector/create', views.create_detector, name='create_detector'),
    path('update/', views.update, name='update'),
    path('ready/<str:pcaId>', views.ready, name='ready'),
    path('start/<str:pcaId>', views.start, name='start'),
    path('stop/<str:pcaId>', views.stop, name='stop'),
    path('shutdown/<str:pcaId>', views.shutdown, name='shutdown'),
    path('setActive/<str:pcaId>',views.setActive, name='setActive' ),
    path('setInactive/<str:pcaId>',views.setInactive, name='setInactive' ),
]
