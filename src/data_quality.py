import os
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def analyze_date_coverage(spark: SparkSession, df_unpivoted: DataFrame, start_date: str, end_date: str, table_name: str = "log_calidad_fechas"):
    """
    Genera un calendario teórico, detecta días faltantes cruzándolos contra la 
    tabla de auditoría de extracción para diagnosticar la causa exacta del vacío.
    """
    catalog_name = os.getenv("CATALOG_NAME", "local")
    full_table_path = f"{catalog_name}.db.{table_name}"
    control_table_path = f"{catalog_name}.db.control_extraccion_log"
    
    print(f"[QUALITY] Analizando cobertura de fechas desde {start_date} hasta {end_date}...")

    diff_dias = (datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days + 1
    if diff_dias <= 0:
        print("!!! [ERROR] La fecha de inicio debe ser anterior a la fecha de fin.")
        return
    
    # 1. Creamos el calendario teórico en memoria con Spark
    df_calendario = spark.range(0, diff_dias) \
        .withColumn("exchange_date", F.date_add(F.to_date(F.lit(start_date)), F.col("id").cast("int"))) \
        .filter(F.col("exchange_date") <= F.to_date(F.lit(end_date))) \
        .select("exchange_date")
        
    # 2. Obtenemos las fechas reales que sí procesó Bronze en este lote
    df_fechas_reales = df_unpivoted.select("exchange_date").distinct()
    
    # 3. Detectamos los huecos usando left_anti para saber qué fechas faltan por completo
    df_gaps = df_calendario.join(df_fechas_reales, on="exchange_date", how="left_anti") \
        .withColumn("day_of_week", F.date_format(F.col("exchange_date"), "E"))
        
    if df_gaps.isEmpty():
        print("🔍 [QUALITY] Cobertura perfecta. No se detectaron días faltantes.")
        return

    # Clasificación inicial por día de la semana
    df_gaps_classified = df_gaps.withColumn(
        "initial_category",
        F.when(F.col("day_of_week").rlike("(?i)(Sat|Sun|Sáb|Dom)"), "FIN_DE_SEMANA")
         .otherwise("PENDIENTE_DIAGNOSTICO")
    )

    # Separamos las que son fin de semana (que ya sabemos la causa)
    df_fines_semana = df_gaps_classified.filter(F.col("initial_category") == "FIN_DE_SEMANA") \
                                        .withColumn("category", F.lit("FIN_DE_SEMANA"))

    # Aislamos los días de la semana que faltaron para investigar por qué
    df_dias_habiles_faltantes = df_gaps_classified.filter(F.col("initial_category") == "PENDIENTE_DIAGNOSTICO")

    if not df_dias_habiles_faltantes.isEmpty() and spark.catalog.tableExists(control_table_path):
        
        # Buscamos el registro de extracción que cubría este periodo de fechas
        df_control = spark.read.table(control_table_path)
        
        # Hacemos un join de proximidad/cobertura: mapeamos si la fecha faltante cayó dentro del rango consultado
        
        df_diagnosticado = df_dias_habiles_faltantes.join(
            df_control,
            (F.col("exchange_date") >= F.to_date(df_control.fecha_inicio_consultado)) &
            (F.col("exchange_date") <= F.to_date(df_control.fecha_fin_consultado)),
            "left"
        ).withColumn(
            "category",
            F.when(F.col("status_code") == "404", "FECHA_SIN_COBERTURA_FESTIVO_O_MERCADO_CERRADO")
             .when(F.col("resultado").rlike("(?i)(NETWORK|TIMEOUT)"), "FECHA_SIN_COBERTURA_FALLA_DE_RED_O_TIMEOUT")
             .otherwise("FECHA_SIN_COBERTURA_DESCONOCIDA_API_NO_DATA")
        ).select("exchange_date", "day_of_week", "category")
    else:
        # Si no hay días hábiles faltantes o no existe la tabla de control todavía, 
        # cae en un estado genérico preventivo
        df_diagnosticado = df_dias_habiles_faltantes.withColumn("category", F.lit("FECHA_SIN_COBERTURA_SIN_LOG")) \
                                                    .select("exchange_date", "day_of_week", "category")

    # 4. Unimos los fines de semana con los días hábiles ya diagnosticados
    df_reporte_final = df_fines_semana.select("exchange_date", "day_of_week", "category") \
        .union(df_diagnosticado) \
        .withColumn("run_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
        .withColumn("checked_at", F.current_timestamp())

    # 5. Persistencia incremental en Apache Iceberg
    if not spark.catalog.tableExists(full_table_path):
        df_reporte_final.writeTo(full_table_path).create()
    else:
        df_reporte_final.writeTo(full_table_path).append()
        
    print(f"[QUALITY ÉXITO] Diagnóstico de cobertura guardado en: {full_table_path}")


def compute_dataset_statistics(spark: SparkSession, df_unpivoted: DataFrame, table_name: str = "log_calidad_metricas"):
    """
    Sección 4.2.5 y 4.2.1: Calcula la radiografía estadística del lote crudo
    utilizando ventanas estadísticas internas sobre los datos en memoria.
    """
    catalog_name = os.getenv("CATALOG_NAME", "local")
    full_table_path = f"{catalog_name}.db.{table_name}"
    
    print("[QUALITY] Evaluando rangos razonables usando estadística interna del lote...")
    
    # 1. Creamos una ventana analítica sobre el lote actual agrupada por moneda
    # Ojo: No ordenamos por fecha porque queremos la foto estadística completa del lote completo
    window_moneda = Window.partitionBy("target_currency")
    
    # 2. Calculamos el promedio y la desviación estándar de la tasa dentro del lote actual
    df_con_limites = df_unpivoted \
        .withColumn("avg_lote", F.avg("exchange_rate").over(window_moneda)) \
        .withColumn("stddev_lote", F.stddev("exchange_rate").over(window_moneda))
        
    # 3. Aplicamos el criterio de bandera de invalidez (Anomalía dura)
    # Criterio: Una tasa es inválida si es <= 0 O si se aleja más de 4 desviaciones estándar 
    # de la media de su propia moneda en este lote (lo cual estadísticamente es un outlier extremo).
    # Si la desviación estándar es perfecta o nula (como en un lote delta de 1 solo día), 
    # el fallback valida que no se desfase más del 50% del promedio del lote.
    df_with_flags = df_con_limites.withColumn(
        "is_invalid",
        F.when(F.col("exchange_rate") <= 0.0, F.lit(1))
         .when(
             (F.col("stddev_lote").isNotNull()) & (F.col("stddev_lote") > 0) & 
             (F.abs(F.col("exchange_rate") - F.col("avg_lote")) > (F.col("stddev_lote") * 4)), 
             F.lit(1)
         )
         .when(
             (F.col("stddev_lote") == 0) & 
             (F.abs(F.col("exchange_rate") - F.col("avg_lote")) / F.col("avg_lote") > 0.50),
             F.lit(1)
         )
         .otherwise(F.lit(0))
    )
    
    # 4. Agregamos las métricas globales para la tabla resumen de auditoría
    # Agrupamos por par de monedas para que la radiografía estadística tenga sentido financiero
    stats_df = df_with_flags.groupBy("base_currency", "target_currency").agg(
        F.count("*").alias("total_records"),
        F.sum(F.when(F.col("exchange_rate").isNull(), 1).otherwise(0)).alias("null_records_count"),
        F.sum("is_invalid").alias("invalid_rates_count"),
        F.min("exchange_rate").alias("min_rate"),
        F.max("exchange_rate").alias("max_rate"),
        F.round(F.avg("exchange_rate"), 4).alias("avg_rate"),
        F.round(F.stddev("exchange_rate"), 4).alias("stddev_rate")
    ).withColumn(
        "run_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S"))
    ).withColumn(
        "execution_timestamp", F.current_timestamp()
    )
    
    # 5. Persistencia del log acumulativo
    if not spark.catalog.tableExists(full_table_path):
        stats_df.writeTo(full_table_path).create()
    else:
        stats_df.writeTo(full_table_path).append()
        
    print(f"[QUALITY] Reporte estadístico guardado con éxito en: {full_table_path}")