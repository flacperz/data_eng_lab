import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime import datetime

def compute_monthly_aggregations(spark: SparkSession, df_enriched: DataFrame):
    """
    Sección 4.2.6: Agrega las métricas mensuales por par de monedas para la tabla resumen de auditoría.
    Garantiza idempotencia aplicando un MERGE por año-mes para evitar duplicados en cargas incrementales.
    """
    print("[AGGREGATIONS] Calculando métricas mensuales de registros activos...")    
    
    # También nos aseguramos de que la tasa no sea nula por si acaso
    df_activos = df_enriched.filter((F.col("is_active") == True) & (F.col("exchange_rate").isNotNull()))
    
    # 1. Creamos la columna de año-mes para agrupar
    df_con_mes = df_activos.withColumn("year_month", F.date_format("exchange_date", "yyyy-MM"))
    
    # 2. Agrupamos por par de monedas y mes para calcular las métricas financieras clave
    monthly_stats_df = df_con_mes.groupBy("base_currency", "target_currency", "year_month").agg(
        F.count("*").alias("total_records"),
        F.min("exchange_rate").alias("min_rate"),
        F.max("exchange_rate").alias("max_rate"),
        F.round(F.avg("exchange_rate"), 4).alias("avg_rate"),
        F.round(F.stddev("exchange_rate"), 4).alias("volatility") # stddev es la volatilidad perfecta
    ).withColumn(
        "run_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S"))
    ).withColumn(
        "execution_timestamp", F.current_timestamp()
    )
    
    # 3. Persistencia robusta en Apache Iceberg
    catalog_name = os.getenv("CATALOG_NAME", "local")
    full_table_path = f"{catalog_name}.db.metricas_mensuales"
    
    #CORRECCIÓN 2: Control de escritura idempotente (MERGE INTO)
    if not spark.catalog.tableExists(full_table_path):
        print(f"[AGGREGATIONS] Creando tabla Gold inicial: {full_table_path}")
        # Al ser Iceberg, podemos particionar la tabla de métricas por el mismo 'year_month'
        monthly_stats_df.writeTo(full_table_path).partitionedBy("year_month").create()
        print(f"Tabla {full_table_path} creada con éxito.")
    else:
        print(f"[AGGREGATIONS] Sincronizando métricas mensuales de forma incremental en {full_table_path}...")
        
        # Registramos el DataFrame de esta corrida como vista temporal
        monthly_stats_df.createOrReplaceTempView("lote_metricas_mensuales")
        
        # Ejecutamos un MERGE para actualizar el mes en curso o insertar si es un mes nuevo
        spark.sql(f"""
            MERGE INTO {full_table_path} target
            USING lote_metricas_mensuales source
            ON target.year_month = source.year_month
               AND target.base_currency = source.base_currency
               AND target.target_currency = source.target_currency
            WHEN MATCHED THEN
                UPDATE SET 
                    target.total_records = source.total_records,
                    target.min_rate = source.min_rate,
                    target.max_rate = source.max_rate,
                    target.avg_rate = source.avg_rate,
                    target.volatility = source.volatility,
                    target.run_id = source.run_id,
                    target.execution_timestamp = source.execution_timestamp
            WHEN NOT MATCHED THEN
                INSERT (base_currency, target_currency, year_month, total_records, min_rate, max_rate, avg_rate, volatility, run_id, execution_timestamp)
                VALUES (source.base_currency, source.target_currency, source.year_month, source.total_records, source.min_rate, source.max_rate, source.avg_rate, source.volatility, source.run_id, source.execution_timestamp)
        """)
        print(f"Métricas mensuales actualizadas sin duplicados en {full_table_path}.")


# 4.- Detección de anomalias                                                              db.anomalias (días con movimientos atípicos)          
#     Identificar días donde la variacion del tipo de cambio supere 2 desviaciones 
#     estandar respecto a su promedio movil de 30 dias.

# target.exchange_rate = source.exchange_rate,
#                     target.moving_avg_7d = source.moving_avg_7d,
#                     target.transformed_at = source.transformed_at,
#                     target.moving_avg_30d = source.moving_avg_30d,
#                     target.rate_yesterday = source.rate_yesterday,
#                     target.variacion_pj_diaria_exrate = source.variacion_pj_diaria_exrate,
#                     target.dev_std_30d = source.dev_std_30d,

def calcular_anomalias(spark: SparkSession, df_enrich: DataFrame):
    
    df_validos = df_enrich.filter(
        (F.col("is_active") == True) & 
        (F.col("dev_std_30d").isNotNull()) &
        (F.col("dev_std_30d") > 0)
    )

    df_anomalias = df_validos.filter(
        F.abs( F.col("variacion_pj_diaria_exrate") / 100) > (2 * F.col("dev_std_30d"))
    ) \
    .select(
        F.col("exchange_date"),
        F.col("base_currency"),
        F.col("target_currency"),
        F.col("exchange_rate"),
        F.col("variacion_pj_diaria_exrate").alias("daily_variation_pct"),
        F.col("moving_avg_7d"),
        F.col("moving_avg_30d").alias("baseline_avg_30d"),
        F.col("dev_std_30d").alias("baseline_stddev_30d")
    ).withColumn(
        "umbral_dev_std_30d", (2 * F.col("baseline_stddev_30d"))
    ).withColumn(
        "run_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S"))
    ).withColumn(
        "execution_timestamp", F.current_timestamp()
    )

    # 4. Persistencia Idempotente en Apache Iceberg
    catalog_name = os.getenv("CATALOG_NAME", "local")
    full_table_path = f"{catalog_name}.db.anomalias"
    
    if not spark.catalog.tableExists(full_table_path):
        print(f"[ANOMALIES] Creando tabla de anomalías inicial: {full_table_path}")
        # Particionamos por año-mes implícito usando meses para optimizar búsquedas históricas
        df_anomalias.writeTo(full_table_path).partitionedBy(F.months("exchange_date")).create()
        print(f"Tabla {full_table_path} creada. Anomalías iniciales registradas.")
    else:
        print(f"[ANOMALIES] Sincronizando nuevas anomalías detectadas en {full_table_path}...")
        
        # Registramos como vista temporal para el Merge
        df_anomalias.createOrReplaceTempView("lote_anomalias_detectadas")
        
        # Ejecutamos el Merge. Si la anomalía ya existía para esa fecha/moneda, actualizamos metadatos.
        # Si es nueva, se inserta.
        spark.sql(f"""
            MERGE INTO {full_table_path} target
            USING lote_anomalias_detectadas source
            ON target.exchange_date = source.exchange_date
               AND target.base_currency = source.base_currency
               AND target.target_currency = source.target_currency
            WHEN MATCHED THEN
                UPDATE SET 
                    target.exchange_rate = source.exchange_rate,
                    target.daily_variation_pct = source.daily_variation_pct,
                    target.baseline_avg_30d = source.baseline_avg_30d,
                    target.baseline_stddev_30d = source.baseline_stddev_30d,
                    target.run_id = source.run_id,
                    target.execution_timestamp = source.execution_timestamp
            WHEN NOT MATCHED THEN
                INSERT (exchange_date, base_currency, target_currency, exchange_rate, daily_variation_pct, baseline_avg_30d, baseline_stddev_30d, run_id, execution_timestamp)
                VALUES (source.exchange_date, source.base_currency, source.target_currency, source.exchange_rate, source.daily_variation_pct, source.baseline_avg_30d, source.baseline_stddev_30d, source.run_id, source.execution_timestamp)
        """)
        print(f"Tabla de anomalías {full_table_path} actualizada correctamente.")
        
    # Retornamos el conteo para los logs informativos del orquestador
    return df_anomalias.count()

