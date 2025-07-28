from django.urls import path
from . import views

urlpatterns = [
    path('', views.pagina_usuarios, name='usuarios_home'),
]