from django.shortcuts import render
from django.http import HttpResponse

def login_view(request):
    return HttpResponse("Página de login")

def registro_view(request):
    return HttpResponse("Página de registro")