from django.shortcuts import render
from django.http import HttpResponse

def pagina_comentarios(request):
    return HttpResponse("Página de Comentarios")