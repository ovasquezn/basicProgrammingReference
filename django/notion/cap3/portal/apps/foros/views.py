from django.shortcuts import render
from django.http import HttpResponse

def pagina_foros(request):
    return HttpResponse("Página de Foros")