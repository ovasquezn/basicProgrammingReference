from django.urls import path
from . import views

urlpatterns = [
    path('', views.pagina_foros, name='foros_home'),
]