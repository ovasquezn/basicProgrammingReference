import pandas as pd
import sqlite3
import logging
import time
from datetime import datetime
import os

# 1. CONFIGURACIÓN DE LOGGING
logging.basicConfig(
    filename="etl_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s"
)

# 2. PARÁMETROS DEL PIPELINE
CSV_FILE = "transacciones.csv"  # Archivo de entrada
DB_FILE = "etl_db.sqlite"       # Base de datos destino
TABLE_NAME = "transacciones"    # Tabla principal
LOG_TABLE = "etl_logs"          # Tabla de logs

# 3. SIMULACIÓN DE ARCHIVO CSV (para pruebas)
if not os.path.exists(CSV_FILE):
    df_sim = pd.DataFrame({
        "id": range(1, 11),
        "cliente": ["Juan", "Ana", "Pedro", None, "Maria", "Luis", None, "Carlos", "Sofia", "Laura"],
        "monto": [100, 200, None, 400, 500, None, 700, 800, None, 1000]
    })
    df_sim.to_csv(CSV_FILE, index=False)

# 4. FUNCIÓN PRINCIPAL ETL
def run_etl():
    start_time = time.time()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Conexión a la base de datos
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Crear tabla principal si no existe
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER,
            cliente TEXT,
            monto REAL
        )
    """)

    # Crear tabla de logs si no existe
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            filas_leidas INTEGER,
            filas_eliminadas INTEGER,
            filas_cargadas INTEGER,
            errores INTEGER,
            duracion REAL
        )
    """)

    # ETAPA 1: EXTRACCIÓN
    try:
        df = pd.read_csv(CSV_FILE)
        filas_leidas = len(df)
        logging.info(f"Archivos leídos: {filas_leidas} registros")
    except Exception as e:
        logging.error(f"Error leyendo archivo: {e}")
        conn.close()
        return

    # ETAPA 2: TRANSFORMACIÓN
    # Eliminamos registros con nulos en cliente o monto
    df_clean = df.dropna(subset=["cliente", "monto"])
    filas_limpias = len(df_clean)
    filas_eliminadas = filas_leidas - filas_limpias

    logging.info(f"Filas eliminadas: {filas_eliminadas}")

    # ETAPA 3: CARGA A BASE DE DATOS
    errores = 0
    try:
        df_clean.to_sql(TABLE_NAME, conn, if_exists="append", index=False)
        filas_cargadas = filas_limpias
        logging.info(f"Filas cargadas: {filas_cargadas}")
    except Exception as e:
        errores += 1
        logging.error(f"Error cargando datos: {e}")
        filas_cargadas = 0

    # REGISTRO EN TABLA DE LOGS
    duracion = round(time.time() - start_time, 2)
    cursor.execute(f"""
        INSERT INTO {LOG_TABLE} (timestamp, filas_leidas, filas_eliminadas, filas_cargadas, errores, duracion)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (timestamp, filas_leidas, filas_eliminadas, filas_cargadas, errores, duracion))

    conn.commit()
    conn.close()

    print("ETL completado")
    print(f"Filas leídas: {filas_leidas}")
    print(f"Filas eliminadas: {filas_eliminadas}")
    print(f"Filas cargadas: {filas_cargadas}")
    print(f"Duración: {duracion} segundos")

# 5. EJECUTAR EL PIPELINE
if __name__ == "__main__":
    run_etl()