from django.urls import path
from . import views

urlpatterns = [
    #path('', views.lista_posts, name='lista_posts'),
    path('', views.hola_blog, name='hola_blog'),  # Ruta principal de blog
    path('<int:id>/', views.detalle_post, name='detalle_post'),
]