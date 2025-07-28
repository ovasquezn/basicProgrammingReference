from django.shortcuts import render
from .models import Articulo

def lista_articulos(request):
    articulos = Articulo.objects.all()
    return render(request, 'articulos/lista_articulos.html', {'articulos': articulos})