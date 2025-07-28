from django.shortcuts import render
from django.http import HttpResponse

def lista_posts(request):
    return HttpResponse("Lista de publicaciones")

def detalle_post(request, id):
    return HttpResponse(f"Detalle del post {id}")

def hola_blog(request):
    return HttpResponse("Hola Blog")