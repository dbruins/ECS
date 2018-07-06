from django.urls import path

from . import views

app_name='GUI'
urlpatterns = [
    path('', views.index, name='index'),
    path('update/', views.update, name='update'),
    path('ready/', views.ready, name='ready'),
    path('start/', views.start, name='start'),
    path('stop/', views.stop, name='stop'),
    path('shutdown/', views.shutdown, name='shutdown'),
]
