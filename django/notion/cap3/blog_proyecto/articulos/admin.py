from django.contrib import admin
from .models import Articulo

#admin.site.register(Articulo)

class ArticuloAdmin(admin.ModelAdmin):
    list_display = ('titulo', 'fecha_creacion')
    search_fields = ('titulo',)
    list_filter = ('fecha_creacion',)

admin.site.register(Articulo, ArticuloAdmin)