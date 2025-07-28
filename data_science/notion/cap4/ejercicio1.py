import requests                     # Para hacer llamadas a APIs (CoinGecko y CryptoPanic)
import pandas as pd
from datetime import datetime

# 1. Extracción desde CoinGecko API (últimos 14 días)
api_url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart" # Endpoint de CoinGecko
params = {"vs_currency": "usd", "days": "14"} # Parámetros: precio en USD, últimos 14 días
response = requests.get(api_url, params=params) # Realizamos la petición GET
data = response.json() # Convertimos la respuesta en un diccionario Python (JSON)

# Extraer precios y fechas
prices = data.get("prices", []) # Cada elemento es [timestamp, price]
df_bitcoin = pd.DataFrame(prices, columns=["timestamp", "price"]) # Crear DataFrame con columnas timestamp y price
df_bitcoin["date"] = pd.to_datetime(df_bitcoin["timestamp"], unit='ms').dt.date # Convertimos timestamp (ms) a fecha (YYYY-MM-DD)
df_bitcoin = df_bitcoin.groupby("date").mean().reset_index() # Agrupamos por fecha para obtener el promedio diario (si hay múltiples registros por día)
# Añadimos columnas para identificar la fuente y el tipo de dato
df_bitcoin["source"] = "API_CoinGecko" # Indica que proviene de CoinGecko
df_bitcoin["type"] = "bitcoin_price"  # Clasificación: precio Bitcoin
# Renombramos la columna 'price' a 'content' para unificar estructura con las noticias
df_bitcoin = df_bitcoin.rename(columns={"price": "content"}) 
# Seleccionamos solo las columnas necesarias
df_bitcoin = df_bitcoin[["date", "content", "source", "type"]]

# 2. Web scraping (Noticias de criptomonedas)
news_url = "https://cryptopanic.com/api/v1/posts/" # Endpoint de CryptoPanic
auth_token = "ea197833c920807b326bda14132c1b7b020020d4"  # Token personal
params_news = {"auth_token": auth_token, "currencies": "BTC", "kind": "news"} # Autenticación, Filtrar por Bitcoin, Solo noticias
news_response = requests.get(news_url, params=params_news) # Petición a la API
news_data = news_response.json().get("results", []) # Obtenemos la lista de noticias


# Convertir noticias a DataFrame
news_list = []
for item in news_data:
    news_list.append({
        "date": item["published_at"][:10],  # Tomamos solo la fecha (YYYY-MM-DD)
        "content": item["title"], # El título de la noticia
        "source": "CryptoPanic", # Fuente
        "type": "news" # Clasificación: noticia
    })

df_news = pd.DataFrame(news_list) # Creamos el DataFrame de noticias

# 3. Combinar ambos datasets
df_combined = pd.concat([df_bitcoin, df_news], ignore_index=True) # Unimos los DataFrames (verticalmente)
df_combined.drop_duplicates(subset=["date", "content"], inplace=True) # Eliminamos duplicados por (fecha + contenido)

# Mostrar resultados
print(df_bitcoin)
print(df_news)
print(df_combined)