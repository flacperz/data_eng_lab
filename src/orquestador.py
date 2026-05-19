import os
from dotenv import load_dotenv
from datetime import datetime
from datetime import timedelta
from pyspark.sql.functions import current_timestamp
from src.extract import save_to_bronze, raw_to_spark
from pyspark.sql import functions as F
from src.data_quality import analyze_date_coverage, compute_dataset_statistics

from src.utils import (
    check_table_exists,
    get_max_date_loaded,
    getSparkSession,
    emit_cdc_events
)
from src.extract import FrankfurterClient

from src.transform import unpivot_bronze_data, apply_forward_fill_and_metrics, save_to_silver, clean_invalid_data, compute_cdc
from src.agregaciones import compute_monthly_aggregations, calcular_anomalias

from src.process_fact_dims import build_dim_time, build_dim_currencies, build_fact_exchange_rates

def run_bronze_pipeline(start_date=None, end_date=None):

    # Cargar variables de entorno
    load_dotenv()
    spark = getSparkSession()
    TABLE_NAME = "tipos_cambio_raw"

    fecha_inicio = os.getenv("START_DATE", "2024-01-01")
    fecha_fin = os.getenv("END_DATE", "2024-06-30")

    today_dt = datetime.today().date()
    client = FrankfurterClient()
    df = None

    # Estrategia de carga

    if start_date is not None and end_date is not None:
        
        fecha_inicio = start_date
        fecha_fin = end_date
        print(f"Se procesará desde la fecha: {fecha_inicio} hasta: {fecha_fin} - Carga por parámetros externos.")    
        
    else:

        if check_table_exists(spark, TABLE_NAME):
            try:
                max_fecha = get_max_date_loaded(spark, TABLE_NAME)
                
                if max_fecha is None or max_fecha == "":                                
                    print(f"Se procesará desde la fecha: {fecha_inicio} hasta: {fecha_fin} - Tabla vacía.")
                elif max_fecha >= today_dt:
                    print("La información en tabla RAW se encuentra al dia. No hay datos nuevos para procesar.")
                    return fecha_inicio, fecha_fin
                else:
                    print(f"Se procesará desde la fecha: {max_fecha + timedelta(days=1)} hasta: {today_dt} - Carga incremental.")
                    fecha_inicio = (max_fecha + timedelta(days=1)).strftime("%Y-%m-%d")
                    fecha_fin = today_dt.strftime("%Y-%m-%d")                

            except Exception as e:
                print(f"Error al obtener fecha maxima de la tabla: {e}")
                return fecha_inicio, fecha_fin
        else:        
            print(f"Se procesará desde la fecha: {fecha_inicio} hasta: {fecha_fin} - Tabla no existe.")
        
    # Consumo de API
    try:
        json_data = client.fetch_range(spark, fecha_inicio, fecha_fin)
    except Exception as e:
        print(f"Error durante la extracción de datos desde API Frankfurter: {e}")
        return fecha_inicio, fecha_fin, 0

    # Validacion de registros
    if not json_data or 'rates' not in json_data:        
        print("Sin datos para procesar.")
        return

    # Procesamiento y carga a Bronze
    try:
        df = raw_to_spark(spark, json_data )
        if df is not None and df.count() > 0:
            conteo_registros_extraidos = df.count()
            print(f"Datos extraídos: {df.count()} registros.")            
            df = df.withColumn("fecha_carga", current_timestamp())            
            save_to_bronze(spark, df)
            return fecha_inicio, fecha_fin, conteo_registros_extraidos
        else:
            print("No se pudo transformar los datos extraídos a DataFrame de Spark.")

    except Exception as e:
        print(f"Error durante la transformación de datos a DataFrame de Spark: {e}")
        return


def run_silver_pipeline(fecha_inicio_silver, fecha_fin_silver):
    spark = getSparkSession()
    print("Iniciando procesamiento incremental de la Capa Silver...")
    
    # 1. Calculamos la fecha de lookback (45 días antes de que empiece el lote nuevo)
    fecha_inicio_dt = datetime.strptime(fecha_inicio_silver, "%Y-%m-%d").date()
    fecha_lookback = (fecha_inicio_dt - timedelta(days=45)).strftime("%Y-%m-%d")
    
    # 2. Leemos de Bronze ÚNICAMENTE desde la fecha de lookback en adelante
    # Esto evita cargar millones de registros del pasado, pero le da contexto a la ventana
    df_bronze_filtrado = spark.read.table("local.db.tipos_cambio_raw") \
                              .filter((F.col("date") >= F.lit(fecha_lookback)) & (F.col("date") <= F.lit(fecha_fin_silver)))
    
    # 3. Mandamos este fragmento optimizado a transformar
    df_unpivoted = unpivot_bronze_data(df_bronze_filtrado)

    # Generar tablas de calidad del dato
    analyze_date_coverage(spark, df_unpivoted, fecha_inicio_silver, fecha_fin_silver)
    compute_dataset_statistics(spark, df_unpivoted)
    
    # Limpieza de datos: Eliminamos o corregimos registros con datos faltantes o inconsistentes
    df_unpivoted = clean_invalid_data(df_unpivoted)
    
    df_silver_ready = apply_forward_fill_and_metrics(df_unpivoted)

    if check_table_exists(spark, "local.db.tipos_cambio_enriquecidos"):
        df_silver_actual = spark.read.table("local.db.tipos_cambio_enriquecidos").filter((F.col("exchange_date") >= F.lit(fecha_lookback)) & (F.col("exchange_date") <= F.lit(fecha_fin_silver)))   
    else:
        df_silver_actual = None

    # Generamos CDC para identificar cambios en los tipos de cambio
    df_cdc = compute_cdc(df_silver_ready, df_silver_actual)

    # Guardar json de eventos para cada cambio detectado por el CDC
    registros_eventos = emit_cdc_events(df_cdc)
    print(f"Eventos generados a partir del CDC: {registros_eventos} registros")

    
    # 4. Guardamos en Silver aplicando MERGE INTO
    # El MERGE se encargará de actualizar o insertar solo este rango en la tabla final
    save_to_silver(spark, df_cdc)
    print(f"Datos cargados desde Bronze para Silver: {df_bronze_filtrado.count()} registros (desde {fecha_inicio_dt} hasta {fecha_fin_silver}).")


    # Calculamos metricas mensuales del lote procesado.
    compute_monthly_aggregations(spark, df_cdc)
    print(f"Metricas generadas, registros procesados: {df_bronze_filtrado.count()} registros (desde {fecha_inicio_dt} hasta {fecha_fin_silver}).")


    # Calculamos anomalías en las tasas de cambio del lote procesado.
    registros_insertados = calcular_anomalias(spark, df_cdc)
    print(f"Anomalías detectadas, registros procesados: {registros_insertados} registros (desde {fecha_inicio_dt} hasta {fecha_fin_silver}).")

def run_gold_pipeline(fecha_inicio, fecha_fin):
    spark = getSparkSession()
    spark.stop()
    spark = getSparkSession()
    print("Iniciando procesamiento de la Capa Gold...")

    # 1. Calculamos la fecha de lookback (45 días antes de que empiece el lote nuevo)
    fecha_inicio_dt = datetime.strptime(fecha_inicio, "%Y-%m-%d").date()    
    fecha_fin_dt = datetime.strptime(fecha_fin, "%Y-%m-%d").date()
    

    # Leemos los datos enriquecidos de Silver para construir las dimensiones y hechos
    df_enriched = spark.read.table("local.db.tipos_cambio_enriquecidos").filter(
        (F.col("exchange_date") >= F.lit(fecha_inicio_dt)) & 
        (F.col("exchange_date") <= F.lit(fecha_fin_dt))
    )

    print(f"Fechas recibidas en capa Gold: fecha_inicio={fecha_inicio}, fecha_fin={fecha_fin}")

    df_fechas_enriched = df_enriched.select(
        F.min("exchange_date").alias("min_date"),
        F.max("exchange_date").alias("max_date")
    ).collect()[0]

    min_date = df_fechas_enriched["min_date"]
    max_date = df_fechas_enriched["max_date"]

    print(f"Fechas en datos enriquecidos: min_date={min_date}, max_date={max_date}")
    
    # Construimos la dimensión de monedas
    build_dim_currencies(spark, df_enriched)
    
    # Construimos la dimensión de tiempo
    build_dim_time(spark, df_enriched)
    
    # Construimos la tabla de hechos con las tasas de cambio y métricas financieras
    build_fact_exchange_rates(spark, df_enriched)