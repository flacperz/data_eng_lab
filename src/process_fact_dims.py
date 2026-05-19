# fact_exchange_rates
# dimension moneda
# dimension tiempo

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime import datetime

def build_dim_currencies(spark: SparkSession, df_enriched: DataFrame):
    """
    Genera la dimensión 'dim_currencies' de forma limpia usando una base fija de datos maestros
    para evitar encadenamientos excesivos de código condicional.
    """
    print("[GOLD] Sincronizando dimensión 'dim_currencies' con catálogo base fijo...")

    # 1. Base inicial fija de datos maestros (Monedas soportadas por Frankfurter/Mercado)
    catalogo_fijo_data = [
        ("USD", "United States Dollar", "North America", True),
        ("EUR", "Euro", "Eurozone", True),
        ("GBP", "British Pound", "Europe (Non-Euro)", True),
        ("CHF", "Swiss Franc", "Europe (Non-Euro)", True),
        ("MXN", "Mexican Peso", "LATAM", False),
        ("BRL", "Brazilian Real", "LATAM", False),
        ("CAD", "Canadian Dollar", "North America", True),
        ("JPY", "Japanese Yen", "Asia-Pacific", True),
        ("CNY", "Chinese Yuan", "Asia-Pacific", False),
        ("AUD", "Australian Dollar", "Asia-Pacific", True)
    ]
    columnas_catalogo = ["currency_code", "currency_name", "currency_region", "is_major_currency"]
    
    # Convertimos la relación fija en un DataFrame al vuelo
    df_catalogo_fijo = spark.createDataFrame(catalogo_fijo_data, schema=columnas_catalogo)

    # 2. Extraer códigos únicos del lote actual (por si aparece una moneda nueva fuera de la base)
    df_monedas_lote = df_enriched.select(F.col("target_currency").alias("currency_code")).distinct() \
        .union(df_enriched.select(F.col("base_currency").alias("currency_code")).distinct()).distinct()

    # 3. Construimos la dimensión final combinando la data con un JOIN por la izquierda
    # Si llega una moneda que no estaba en el catálogo fijo, no truena: se le asigna "Other" mediante coalesce
    df_dim_final = df_monedas_lote.join(df_catalogo_fijo, "currency_code", "left").withColumn(
        "currency_name", F.coalesce(F.col("currency_name"), F.lit("Unknown / Other"))
    ).withColumn(
        "currency_region", F.coalesce(F.col("currency_region"), F.lit("International / Other"))
    ).withColumn(
        "is_major_currency", F.coalesce(F.col("is_major_currency"), F.lit(False))
    ).withColumn(
        "is_active", F.lit(True)
    ).withColumn(
        "updated_at", F.current_timestamp()
    )

    # 4. Persistencia Idempotente en Iceberg (Mismo MERGE INTO)
    catalog_name = os.getenv("CATALOG_NAME", "local")
    full_table_path = f"{catalog_name}.db.dim_currencies"
    
    if not spark.catalog.tableExists(full_table_path):
        print(f"[GOLD] Creando tabla física inicial: {full_table_path}")
        df_dim_final.writeTo(full_table_path).create()
    else:
        df_dim_final.createOrReplaceTempView("lote_dim_currencies")
        spark.sql(f"""
            MERGE INTO {full_table_path} target
            USING lote_dim_currencies source
            ON target.currency_code = source.currency_code
            WHEN MATCHED THEN
                UPDATE SET 
                    target.currency_name = source.currency_name,
                    target.currency_region = source.currency_region,
                    target.is_major_currency = source.is_major_currency,
                    target.is_active = source.is_active,
                    target.updated_at = source.updated_at
            WHEN NOT MATCHED THEN
                INSERT (currency_code, currency_name, currency_region, is_major_currency, is_active, updated_at)
                VALUES (source.currency_code, source.currency_name, source.currency_region, source.is_major_currency, source.is_active, source.updated_at)
        """)
    print("Dimensión 'dim_currencies' actualizada limpiamente.")




def build_dim_time(spark: SparkSession, df_enriched: DataFrame):
    """
    Genera y actualiza la dimensión 'dim_time' de forma dinámica en la capa Gold.
    Calcula el rango de fechas basándose en los datos históricos del lote actual.
    """
    print("[GOLD] Sincronizando dimensión de tiempo 'dim_time'...")

    # 1. Obtener la fecha mínima y máxima del lote para saber qué rango cubrir
    rango_fechas = df_enriched.select(
        F.min("exchange_date").alias("min_date"),
        F.max("exchange_date").alias("max_date")
    ).collect()[0]
    
    min_date = rango_fechas["min_date"]
    max_date = rango_fechas["max_date"]
    
    if not min_date or not max_date:
        print("[GOLD] No se encontraron fechas válidas para construir la dimensión de tiempo.")
        return

    # 2. Generar una serie consecutiva de fechas usando funciones nativas de Spark
    # sequence() crea un array de fechas desde min hasta max, explode() lo convierte en filas
    df_base_tiempo = spark.range(1).select(
        F.explode(F.sequence(F.lit(min_date), F.lit(max_date), F.expr("INTERVAL 1 DAY"))).alias("date_id")
    )    

    # 1. Creamos las listas de strings para el mapeo nativo en SQL
    meses_lista = ["'Enero'", "'Febrero'", "'Marzo'", "'Abril'", "'Mayo'", "'Junio'", 
                    "'Julio'", "'Agosto'", "'Septiembre'", "'Octubre'", "'Noviembre'", "'Diciembre'"]

    meses_cortos_lista = ["'Ene'", "'Feb'", "'Mar'", "'Abr'", "'May'", "'Jun'", 
                        "'Jul'", "'Ago'", "'Sep'", "'Oct'", "'Nov'", "'Dic'"]

    dias_lista = ["'Domingo'", "'Lunes'", "'Martes'", "'Miércoles'", "'Jueves'", "'Viernes'", "'Sábado'"]

    # 2. Aplicamos al DataFrame usando F.expr y la función ELT
    df_dim_time = df_base_tiempo.withColumn(
        "year", F.year(F.col("date_id"))
    ).withColumn(
        "month", F.month(F.col("date_id"))
    ).withColumn(        
        "month_name", F.expr(f"ELT(month, {', '.join(meses_lista)})")
    ).withColumn(
        "month_short_name", F.expr(f"ELT(month, {', '.join(meses_cortos_lista)})")
    ).withColumn(
        "quarter", F.quarter(F.col("date_id"))
    ).withColumn(
        "day_of_week", F.dayofweek(F.col("date_id"))
    ).withColumn(        
        "day_name", F.expr(f"ELT(day_of_week, {', '.join(dias_lista)})")
    ).withColumn(
        "is_weekend", F.col("day_of_week").isin([1, 7])
    ).withColumn(
        "updated_at", F.current_timestamp()
    )

    # 4. Persistencia Idempotente en el catálogo de Apache Iceberg
    catalog_name = os.getenv("CATALOG_NAME", "local")
    full_table_path = f"{catalog_name}.db.dim_time"

    if not spark.catalog.tableExists(full_table_path):
        print(f"[GOLD] Creando tabla física inicial para el tiempo: {full_table_path}")
        # Particionamos por año para mantener los archivos agrupados si la historia crece a décadas
        df_dim_time.writeTo(full_table_path).partitionedBy("year").create()
    else:
        print(f"[GOLD] Ejecutando MERGE INTO en {full_table_path}...")
        df_dim_time.createOrReplaceTempView("lote_dim_time")
        
        spark.sql(f"""
            MERGE INTO {full_table_path} target
            USING lote_dim_time source
            ON target.date_id = source.date_id
            WHEN MATCHED THEN
                UPDATE SET 
                    target.year = source.year,
                    target.month = source.month,
                    target.month_short_name = source.month_short_name,
                    target.quarter = source.quarter,
                    target.day_of_week = source.day_of_week,
                    target.is_weekend = source.is_weekend,
                    target.updated_at = source.updated_at
            WHEN NOT MATCHED THEN
                INSERT (date_id, year, month, month_short_name, quarter, day_of_week, is_weekend, updated_at)
                VALUES (source.date_id, source.year, source.month, source.month_short_name, source.quarter, source.day_of_week, source.is_weekend, source.updated_at)
        """)
    print("Dimensión 'dim_time' actualizada con éxito.")


import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

def build_fact_exchange_rates(spark: SparkSession, df_enriched: DataFrame):
    """
    Genera y actualiza la tabla de hechos 'fact_exchange_rates' en la capa Gold.
    Estructura las métricas numéricas y claves foráneas para el esquema en estrella.
    """
    print("[GOLD] Sincronizando tabla de hechos 'fact_exchange_rates'...")

    df_fechas_enriched = df_enriched.select(
        F.min("exchange_date").alias("min_date"),
        F.max("exchange_date").alias("max_date")
    ).collect()[0]

    min_date = df_fechas_enriched["min_date"]
    max_date = df_fechas_enriched["max_date"]

    print(f"Fechas en datos enriquecidos: min_date={min_date}, max_date={max_date} antes del filtro por is_active")

    # 1. Filtrar solo registros válidos y activos (Ignorar borrados lógicos del CDC)
    df_activos = df_enriched.filter((F.col("is_active") == True))
    
    df_fechas_activos = df_activos.select(
        F.min("exchange_date").alias("min_date"),
        F.max("exchange_date").alias("max_date")
    ).collect()[0]
    min_date_activos = df_fechas_activos["min_date"]
    max_date_activos = df_fechas_activos["max_date"]
    print(f"Fechas en datos activos: min_date={min_date_activos}, max_date={max_date_activos}")

    # 2. Generamos el run_id y el timestamp para esta corrida de Gold
    run_id_actual = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    df_activos = df_activos.withColumn(
        "run_id", F.lit(run_id_actual)
    ).withColumn(
        "execution_timestamp", F.current_timestamp()
    )

    # 2. Seleccionar, renombrar y formatear las columnas para el estándar de Hechos
    df_fact_final = df_activos.select(
        # Claves Foráneas (FK) que se conectarán con dim_time y dim_currencies
        F.col("exchange_date").alias("date_id"),
        F.col("base_currency").alias("base_currency_code"),
        F.col("target_currency").alias("target_currency_code"),
        
        # Hechos / Métricas Numéricas
        F.round(F.col("exchange_rate"), 4).alias("exchange_rate"),
        F.round(F.col("variacion_pj_diaria_exrate"), 4).alias("daily_variation_pct"),
        F.round(F.col("moving_avg_30d"), 4).alias("moving_avg_30d"),
        F.round(F.col("dev_std_30d"), 4).alias("dev_std_30d"),
        
        # Contexto Analítico y Linaje (Valor Real)
        F.when(F.col("is_originally_null") == 1, F.lit("FORWARD_FILLED"))
         .otherwise(F.lit("REAL")).alias("source_data_status"),
        F.col("run_id"),
        F.col("execution_timestamp")
    )



    # 3. Persistencia Idempotente en el catálogo de Apache Iceberg
    catalog_name = os.getenv("CATALOG_NAME", "local")
    full_table_path = f"{catalog_name}.db.fact_exchange_rates"

    if not spark.catalog.tableExists(full_table_path):
        print(f"[GOLD] Creando tabla física inicial para los hechos: {full_table_path}")
        # Particionamos de forma razonada por año-mes implícito (meses) usando las bondades de Iceberg
        # Esto optimiza queries históricos masivos sin caer en over-partitioning
        df_fact_final.writeTo(full_table_path).partitionedBy(F.months("date_id")).create()
    else:
        print(f"[GOLD] Ejecutando MERGE INTO en tabla de hechos {full_table_path}...")
        df_fact_final.createOrReplaceTempView("lote_fact_exchange_rates")
        
        spark.sql(f"""
            MERGE INTO {full_table_path} target
            USING lote_fact_exchange_rates source
            ON target.date_id = source.date_id
               AND target.base_currency_code = source.base_currency_code
               AND target.target_currency_code = source.target_currency_code
            WHEN MATCHED THEN
                UPDATE SET 
                    target.exchange_rate = source.exchange_rate,
                    target.daily_variation_pct = source.daily_variation_pct,
                    target.moving_avg_30d = source.moving_avg_30d,
                    target.dev_std_30d = source.dev_std_30d,
                    target.source_data_status = source.source_data_status,
                    target.run_id = source.run_id,
                    target.execution_timestamp = source.execution_timestamp
            WHEN NOT MATCHED THEN
                INSERT (date_id, base_currency_code, target_currency_code, exchange_rate, daily_variation_pct, moving_avg_30d, dev_std_30d, source_data_status, run_id, execution_timestamp)
                VALUES (source.date_id, source.base_currency_code, source.target_currency_code, source.exchange_rate, source.daily_variation_pct, source.moving_avg_30d, source.dev_std_30d, source.source_data_status, source.run_id, source.execution_timestamp)
        """)
    print(f"[GOLD] Tabla de hechos 'fact_exchange_rates' actualizada con éxito.")