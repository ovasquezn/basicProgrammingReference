from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, countDistinct, to_date, month, year
import time
from pyspark.sql.functions import lit, when, floor, rand
import os
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType

# 1. CREAR SESIÓN SPARK
spark = SparkSession.builder \
    .appName("VentasPorPaisYMes") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# 2. SIMULAR ARCHIVOS PARQUET (si no existen)

parquet_path = "ventas_parquet"

if not os.path.exists(parquet_path):
    print("Generando datos simulados (~10GB)...")

    schema = StructType([
        StructField("cliente_id", IntegerType(), True),
        StructField("pais", StringType(), True),
        StructField("fecha", DateType(), True),
        StructField("monto", DoubleType(), True)
    ])

    # Simulamos ~10 millones de registros (ajustable para +10GB reales)
    data = spark.range(0, 10_000_000) \
        .withColumn("cliente_id", (floor(rand() * 1_000_000)).cast("int")) \
        .withColumn("pais", (floor(rand() * 5)).cast("int")) \
        .withColumn("fecha", (lit("2023-01-01").cast("date") + floor(rand() * 365).cast("int"))) \
        .withColumn("monto", (rand() * 1000).cast("double")) \
        #.replace({0: "Chile", 1: "México", 2: "Argentina", 3: "Perú", 4: "Colombia"}, subset=["pais"])

    data = data.withColumn(
        "pais",
        when(col("pais") == 0, "Chile")
        .when(col("pais") == 1, "México")
        .when(col("pais") == 2, "Argentina")
        .when(col("pais") == 3, "Perú")
        .when(col("pais") == 4, "Colombia")
        .otherwise("Otro")
    )
    data.write.mode("overwrite").parquet(parquet_path)
    print("Datos generados y guardados como Parquet")


# 3. CARGAR ARCHIVOS PARQUET
start_time = time.time()

df = spark.read.parquet(parquet_path)

# Cachamos los datos en memoria para evitar múltiples lecturas
df.cache()


# 4. PROCESAMIENTO AGREGADO
# Extraemos mes y año
df = df.withColumn("mes", month(col("fecha"))).withColumn("anio", year(col("fecha")))

# Agrupamos y agregamos
agg_df = df.groupBy("pais", "anio", "mes") \
    .agg(
        _sum("monto").alias("total_ventas"),
        countDistinct("cliente_id").alias("clientes_unicos")
    ) \
    .orderBy("pais", "anio", "mes")


# 5. MOSTRAR Y GUARDAR RESULTADO
agg_df.show(truncate=False)

agg_df.write.mode("overwrite").parquet("resumen_ventas_pais_mes")

print("Consulta completada")
print(f"Tiempo total: {round(time.time() - start_time, 2)} segundos")


# 6. CERRAR SPARK
spark.stop()