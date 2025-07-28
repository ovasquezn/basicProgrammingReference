from django.urls import path
from . import views

urlpatterns = [
    path('', views.pagina_comentarios, name='comentarios_home'),
]