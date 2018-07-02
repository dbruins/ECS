from django.urls import path

from . import views

app_name='GUI'
urlpatterns = [
    path('', views.index, name='index'),
    path('update/', views.update, name='update'),
    path('logs/', views.logUpdate, name='logs'),
    path('ready/', views.ready, name='ready'),
    path('start/', views.start, name='start'),
    path('stop/', views.stop, name='stop'),
    path('shutdown/', views.shutdown, name='shutdown'),
    # ex: /polls/5/
    path('<int:question_id>/', views.detail, name='detail'),
    # ex: /polls/5/results/
    path('<int:question_id>/results/', views.results, name='results'),
    # ex: /polls/5/vote/
    path('<int:question_id>/vote/', views.vote, name='vote'),
]
