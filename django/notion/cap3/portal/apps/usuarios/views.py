from django.shortcuts import render
from django.http import HttpResponse

def pagina_usuarios(request):
    return HttpResponse("Página de Usuarios")