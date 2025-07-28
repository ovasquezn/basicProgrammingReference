import pandas as pd
import numpy as np
import dask.dataframe as dd
from dask.diagnostics import ProgressBar

# 1. SIMULAR UN DATASET GRANDE (1 millón de filas)
np.random.seed(42)
n = 1_000_000  # Número de registros

# Simulamos las columnas del CSV
data = {
    "cliente_id": np.random.randint(1, 100_000, n),                  # IDs aleatorios
    "monto": np.random.normal(500, 200, n),                          # Monto con distribución normal
    "categoria": np.random.choice(["A", "B", "C", "D", "E"], n),     # Categoría simple
    "region": np.random.choice([f"Region_{i}" for i in range(1, 501)], n)  # Alta cardinalidad
}

# Introducimos valores faltantes en la columna "monto"
missing_indices = np.random.choice(n, size=5000, replace=False)
data["monto"][missing_indices] = np.nan

# Creamos el DataFrame y lo guardamos como CSV para simular entrada real
df = pd.DataFrame(data)
csv_path = "transactions.csv"
df.to_csv(csv_path, index=False)

print("CSV simulado creado:", csv_path)


# 2. CARGAR EL CSV CON DASK
# Dask lee en particiones para manejar datos grandes
ddf = dd.read_csv(csv_path)

# 3. IMPUTACIÓN DE VALORES FALTANTES (mediana por grupo 'categoria')
# Calculamos mediana por categoría (necesitamos .compute() porque es un cálculo)
medianas = ddf.groupby("categoria")["monto"].median().compute()
medianas_dict = medianas.to_dict()  # Lo convertimos a diccionario
# Definimos función para reemplazar valores NaN con la mediana del grupo
def imputar_monto(row):
    if np.isnan(row["monto"]):
        return medianas_dict[row["categoria"]]
    return row["monto"]

# Aplicamos imputación usando apply
ddf["monto"] = ddf.apply(imputar_monto, axis=1, meta=("monto", "float64"))

# 4. DETECCIÓN Y RECORTE DE OUTLIERS (Winsorización)
# Calculamos percentiles 1% y 99% para recortar extremos
q1, q99 = ddf["monto"].quantile([0.01, 0.99]).compute()

# Función para winsorizar valores
def winsorize(value):
    if value < q1:
        return q1
    elif value > q99:
        return q99
    return value

# Aplicamos la función sobre la columna monto
ddf["monto"] = ddf["monto"].map(winsorize, meta=("monto", "float64"))

# 5. ONE-HOT ENCODING PARA COLUMNA DE ALTA CARDINALIDAD (region)
# Convertimos a categoría para ahorrar memoria
ddf["region"] = ddf["region"].astype("category")

# Identificamos las 10 regiones más frecuentes
top_regions = ddf["region"].value_counts().nlargest(10).compute().index.tolist()

# Agrupamos regiones menos frecuentes en 'OTHER'
def encode_region(region):
    return region if region in top_regions else "OTHER"

ddf["region_grouped"] = ddf["region"].map(encode_region, meta=("region_grouped", "object"))

# Convertimos a categoría
ddf["region_grouped"] = ddf["region_grouped"].astype("category")

# Aseguramos que Dask conozca todas las categorías
ddf = ddf.categorize(columns=["region_grouped"])

# One-hot encoding
dummies = dd.get_dummies(ddf["region_grouped"], prefix="region")
ddf = ddf.join(dummies)


# 6. EJECUTAMOS EL PIPELINE
# Usamos barra de progreso para seguimiento
with ProgressBar():
    result = ddf.compute()  # Ejecutamos todo el pipeline en paralelo

# 7. MOSTRAMOS RESULTADOS
print("Preprocesamiento completado")
print(result.head(20))

# Guardamos resultado final (opcional)
result.to_csv("transactions_clean.csv", index=False)
print("Archivo limpio guardado en: transactions_clean.csv")