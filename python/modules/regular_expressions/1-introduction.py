import re

def buscar_telefono(texto):
    # Expresión regular para encontrar números de teléfono en formato 123-456-7890
    patron = r"\d{3}-\d{3}-\d{4}"
    resultado = re.search(patron, texto)
    if resultado:
        return resultado.group()
    else:
        return None

texto = "Puedes llamarme al 987-654-3210 o al 123-456-7890 mañana."
telefono_encontrado = buscar_telefono(texto)
if telefono_encontrado:
    print(f"Número de teléfono encontrado: {telefono_encontrado}")
else:
    print("No se encontró ningún número de teléfono en el texto.")

def buscar_todos_los_correo(texto):
    # Expresión regular para encontrar direcciones de correo electrónico

    
    patron_1 = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
    patron_2 = r"\b[\w\.-]+@[\w\.-]+\.\w+\b"
    resultados = re.findall(patron_1, texto)
    return resultados

texto = "Contacta a soporte@empresa.com o ventas_soporte@tienda.cl para más información."

correos_encontrados = buscar_todos_los_correo(texto)
if correos_encontrados:
    print("Correos electrónicos encontrados:")
    for correo in correos_encontrados:
        print(correo)
else:
    print("No se encontraron correos electrónicos en el texto.")


