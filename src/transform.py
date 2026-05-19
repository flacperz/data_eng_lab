import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, DoubleType, StringType
from src.utils import getSparkSession, check_table_exists

def unpivot_bronze_data(df_bronze: DataFrame) -> DataFrame:
    """
    Transforma el DataFrame de la capa Bronce (formato ancho) a un formato largo (unpivot).
    Esto facilita el análisis y la carga en capas posteriores.
    """
    # Identificar las columnas de moneda (todas excepto 'date', 'base' y 'fecha_carga')
    columnas_no_unpivot = ['date', 'base', 'fecha_carga']
    monedas = [col for col in df_bronze.columns if col not in columnas_no_unpivot]

    # Construimos la expresión de asignación para la función stack de Spark
    # Ejemplo resultante: "stack(3, 'MXN', MXN, 'EUR', EUR, 'CAD', CAD)"
    stack_expr = ", ".join([f"'{m}', {m}" for m in monedas])
    print("Construyendo expresión de unpivot: ", stack_expr)

    unpivot_query = f"stack({len(monedas)}, {stack_expr}) as (target_currency, exchange_rate)"
    print("Expresión final de unpivot: ", unpivot_query)
    return df_bronze.select(
        F.col("date").alias("exchange_date"),
        F.col("base").alias("base_currency"),
        F.expr(unpivot_query)
    )


def clean_invalid_data(df_unpivoted: DataFrame) -> DataFrame:
    """
    Sección 4.2.1: Limpieza Defensiva del Dataset.
    Asegura tipos de datos, elimina duplicados, remueve nulos y valida rangos razonables.
    """
    print("[TRANSFORM] Iniciando limpieza profunda de datos crudos...")

    # 1. Asegurar tipos de datos correctos (Casteo explícito estricto)
    df_typed = df_unpivoted \
        .withColumn("exchange_date", F.col("exchange_date").cast(DateType())) \
        .withColumn("base_currency", F.col("base_currency").cast(StringType())) \
        .withColumn("target_currency", F.col("target_currency").cast(StringType())) \
        .withColumn("exchange_rate", F.col("exchange_rate").cast(DoubleType()))

    # 2. Manejo de nulos (Eliminar registros donde las llaves de negocio o la tasa sean NULL)
    # Si la tasa viene nula desde la API cruda, no podemos calcular métricas sobre ella aún.
    df_no_nulls = df_typed.dropna(subset=["exchange_date", "base_currency", "target_currency", "exchange_rate"])

    # 3. Validar rangos de tasas (Tasas estrictamente positivas y dentro de límites razonables)
    # Por ejemplo, una tasa no puede ser <= 0, ni tampoco un número absurdamente gigante por un error de typo de la API (ej. > 10,000)
    df_valid_range = df_no_nulls.filter(
        (F.col("exchange_rate") > 0.0) & 
        (F.col("exchange_rate") < 10000.0)
    )

    # 4. Eliminar duplicados de la llave compuesta (Fecha + Base + Target)
    # Si por algún error de la extracción se duplicó un registro, nos quedamos con el único/primero
    df_clean = df_valid_range.dropDuplicates(["exchange_date", "base_currency", "target_currency"])

    print(f"[TRANSFORM] Limpieza finalizada con éxito.")
    return df_clean


def apply_forward_fill_and_metrics(df_long: DataFrame) -> DataFrame:
    """
    Aplica operaciones de ventana para asegurar la continuidad de los datos
    y calcula la media móvil de 7 días para las cotizaciones.
    """

    spark = getSparkSession()

    # 1. Obtener el rango de fechas real que viene en el DataFrame
    rango_fechas = df_long.select(
        F.min("exchange_date").alias("min_date"), 
        F.max("exchange_date").alias("max_date")
    ).collect()[0]
    
    min_date, max_date = rango_fechas["min_date"], rango_fechas["max_date"]
    
    if not min_date or not max_date:
        return df_long


    # ---Borrado logico prev
    # 1. Creamos la ventana por moneda ordenada por fecha para el relleno
    window_ff = Window.partitionBy("base_currency", "target_currency") \
                      .orderBy("exchange_date") \
                      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    # 2. Guardamos una bandera ANTES del Forward-Fill para saber si el dato original era nulo
    df_with_null_flag = df_long.withColumn(
        "is_originally_null", 
        F.when(F.col("exchange_rate").isNull(), F.lit(1)).otherwise(F.lit(0))
    )
    
    # 3. Aplicamos el Forward-Fill clásico a la tasa para parchar los días vacíos
    df_filled = df_with_null_flag.withColumn(
        "exchange_rate", 
        F.last("exchange_rate", ignorenulls=True).over(window_ff)
    )
    
    # 4. TRUCO DE VENTANA: Contamos los días consecutivos que han sido rellenados
    # Si el dato es real (is_originally_null = 0), el contador se reinicia a 0.
    # Si es rellenado, se suma al conteo anterior.
    window_streak = Window.partitionBy("base_currency", "target_currency").orderBy("exchange_date")
    
    # Usamos una expresión condicional nativa para acumular la racha de nulos
    df_with_streak = df_filled.withColumn(
        "consecutive_filled_days",
        F.sum("is_originally_null").over(window_ff) - 
        F.max(F.when(F.col("is_originally_null") == 0, F.sum("is_originally_null").over(window_ff)).otherwise(0)).over(window_ff)
    )
    # -----

    dias_diferencia = (max_date - min_date).days + 1

    # 2. CREACIÓN DEL CALENDARIO: Generamos una secuencia diaria sin huecos
    df_calendario = spark.range(0, dias_diferencia).withColumnRenamed("id", "dias_sumar") \
                         .withColumn("exchange_date", F.date_add(F.lit(min_date), F.col("dias_sumar").cast("int"))) \
                         .select("exchange_date")

    # 3. Obtener combinaciones únicas de monedas base y destino en el lote actual
    df_monedas_unicas = df_long.select("base_currency", "target_currency").distinct()

    # 4. Producto cartesiano para tener una fila por cada día para cada par de monedas
    df_esqueleto_completo = df_calendario.crossJoin(df_monedas_unicas)

    # 5. Cruzamos el esqueleto contra tus datos reales para que aparezcan los "Nulls" de los fines de semana
    df_con_huecos = df_esqueleto_completo.join(
        df_long, 
        on=["exchange_date", "base_currency", "target_currency"], 
        how="left"
    )
    
    # 1. Definimos la especificación de la ventana base
    # Particionamos por el par de monedas (ej. USD-MXN) y ordenamos cronológicamente
    window_spec = Window.partitionBy("base_currency", "target_currency") \
                        .orderBy("exchange_date")
                        
    # 2. Mecanismo de Forward-Fill (Defensivo)
    # Si la API llega a omitir un día o si cruzas esto contra un calendario completo,
    # 'last' con 'ignorenulls=True' arrastra el último valor válido conocido hacia adelante.
    last_valid_rate = F.last("exchange_rate", ignorenulls=True).over(window_spec)
    
    # Flag para identificar si el valor fue rellenado
    df_filled = df_con_huecos.withColumn(
        "is_originally_null", 
        F.when(F.col("exchange_rate").isNull(), F.lit(1)).otherwise(F.lit(0))
    )

    df_filled = df_filled.withColumn("exchange_rate", F.coalesce(F.col("exchange_rate"), last_valid_rate))

    
    # 3. Ventana móvil para la Media Móvil (Moving Average)
    # Explicación técnica: 'rowsBetween(-6, 0)' le dice a Spark que calcule
    # el promedio usando: las 6 filas anteriores + la fila en la que está parado actualmente (7 días en total).
    window_moving_7_days_avg = window_spec.rowsBetween(-6, 0)
    window_moving_30_days_avg = window_spec.rowsBetween(-29, 0)    
    
    # 4. Cálculo de la métrica y adición de marca de tiempo de auditoría Silver
    df_silver = df_filled.withColumn(
        "moving_avg_7d",
        F.round(F.avg("exchange_rate").over(window_moving_7_days_avg), 4)
    ).withColumn(
        "transformed_at", 
        F.current_timestamp()
    )
    
    df_silver = df_silver.withColumn(
        "moving_avg_30d",
        F.round(F.avg("exchange_rate").over(window_moving_30_days_avg), 4)
    )

    df_silver = df_silver.withColumn(
        "rate_yesterday",
        F.lag("exchange_rate", 1).over(window_spec)
    ).withColumn(
        "variacion_pj_diaria_exrate",
        F.round((F.col("exchange_rate") - F.col("rate_yesterday")) / F.col("rate_yesterday") * 100, 4)
    )

    df_silver= df_silver.withColumn(
        "dev_std_30d",
        F.round(F.stddev("exchange_rate").over(window_moving_30_days_avg), 4)
    )
    
    return df_silver

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pyspark.sql import Window
from pyspark.sql import functions as F

def compute_cdc(df_nuevo_enriquecido, df_silver_actual):
    print("[TRANSFORM] Ejecutando detección de cambios (CDC) con ventana de Borrado Lógico...")

    # 1. Definimos la ventana analítica por par de monedas ordenada cronológicamente
    window_acumulada = Window.partitionBy("base_currency", "target_currency") \
                             .orderBy("exchange_date") \
                             .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # 2. Calculamos la racha de días rellenados consecutivamente en el lote actual
    # Usamos la suma acumulada de 'is_originally_null'. Para reiniciar el contador cuando aparece un dato real (0),
    # restamos el valor máximo acumulado hasta el último registro real.
    df_con_conteo = df_nuevo_enriquecido.withColumn(
        "racha_rellenado",
        F.sum("is_originally_null").over(window_acumulada) - 
        F.max(F.when(F.col("is_originally_null") == 0, F.sum("is_originally_null").over(window_acumulada)).otherwise(0)).over(window_acumulada)
    )

    # Escenario A: Primera corrida (Silver vacía)
    if df_silver_actual is None or df_silver_actual.isEmpty():
        print("[CDC] Carga inicial. Mapeando operaciones base...")
        return df_con_conteo \
            .withColumn("is_active", F.when(F.col("racha_rellenado") > 7, F.lit(False)).otherwise(F.lit(True))) \
            .withColumn("operation_type", F.when(F.col("is_active") == False, F.lit("LOGICAL_DELETE")).otherwise(F.lit("INSERT"))) \
            .withColumn("ingestion_timestamp", F.current_timestamp()) \
            .withColumn("updated_at", F.current_timestamp())

    # Escenario B: Proceso Incremental (Comparación contra el histórico)
    df_hist_check = df_silver_actual.select(
        F.col("exchange_date").alias("hist_date"),
        F.col("base_currency").alias("hist_base"),
        F.col("target_currency").alias("hist_target"),
        F.col("exchange_rate").alias("hist_rate"),
        F.col("is_active").alias("hist_active"),
        F.col("ingestion_timestamp").alias("hist_ingestion")
    )

    df_compared = df_con_conteo.join(
        df_hist_check,
        (df_con_conteo.exchange_date == df_hist_check.hist_date) &
        (df_con_conteo.base_currency == df_hist_check.hist_base) &
        (df_con_conteo.target_currency == df_hist_check.hist_target),
        "left"
    )

    # 3. Clasificación estricta de tipos de operación incluyendo el Borrado Lógico
    df_cdc = df_compared.withColumn(
        "is_active",
        F.when(F.col("racha_rellenado") > 7, F.lit(False)).otherwise(F.lit(True))
    ).withColumn(
        "operation_type",
        F.when(F.col("hist_date").isNull() & (F.col("is_active") == False), F.lit("LOGICAL_DELETE"))
         .when(F.col("hist_date").isNull(), F.lit("INSERT"))
         # Si en Silver estaba activo pero la racha superó el límite, se marca el borrado lógico
         .when((F.col("hist_active") == True) & (F.col("is_active") == False), F.lit("LOGICAL_DELETE"))
         # Si la tasa cambió o si una moneda previamente desactivada vuelve a tener datos reales
         .when((F.col("exchange_rate") != F.col("hist_rate")) | (F.col("hist_active") != F.col("is_active")), F.lit("UPDATE"))
         .otherwise(F.lit("NO_CHANGE"))
    ).withColumn(
        "ingestion_timestamp",
        F.when(F.col("operation_type").isin("UPDATE", "LOGICAL_DELETE"), F.col("hist_ingestion"))
         .otherwise(F.current_timestamp())
    ).withColumn(
        "updated_at", F.current_timestamp()
    )

    # Filtramos para quedarnos solo con lo que aporta cambios físicos a Silver
    df_final_lote = df_cdc.filter(F.col("operation_type") != "NO_CHANGE") \
                          .drop("hist_date", "hist_base", "hist_target", "hist_rate", "hist_active", "hist_ingestion")

    print(f"[CDC] Detección finalizada con ventana de control. Cambios a aplicar: {df_final_lote.count()}")
    return df_final_lote

def save_to_silver(spark: SparkSession, df_silver: DataFrame, table_name: str = "tipos_cambio_enriquecidos"):
    """
    Persiste los datos transformados en la capa Silver de Apache Iceberg 
    garantizando la idempotencia mediante una operación MERGE (Upsert).
    """

    if len(table_name.split('.')) != 3:
        local_table_name = f"local.db.{table_name}"
    else:
        local_table_name = table_name
    
    # 2. ESCENARIO A: Si la tabla NO existe, la creamos por primera vez
    if not check_table_exists(spark, local_table_name):
        print(f">>> [SILVER] Creando tabla '{local_table_name}' por primera vez...")
        
        # Guardamos en formato Parquet optimizado para Iceberg
        df_silver.writeTo(local_table_name) \
                .partitionedBy(F.months("exchange_date")) \
                .tableProperty("write.format.default", "parquet") \
                .create()
                 
    # 3. ESCENARIO B: Si la tabla SÍ existe, aplicamos un MERGE INTO (Upsert)
    else:
        print(f">>> [SILVER] Tabla detectada. Aplicando MERGE incremental en: {local_table_name}")
        
        # Registramos el DataFrame de Spark como una vista temporal para poder usar SQL puro
        df_silver.createOrReplaceTempView("silver_incremental_view")

        # La llave compuesta para identificar un registro único en Silver es de 3 campos:
        # La fecha + la moneda base + la moneda destino.
        merge_query = f"""
            MERGE INTO {local_table_name} t
            USING silver_incremental_view s
            ON 
                t.exchange_date = s.exchange_date 
            AND t.base_currency = s.base_currency 
            AND t.target_currency = s.target_currency
            
            WHEN MATCHED AND (t.exchange_rate != s.exchange_rate OR t.operation_type != s.operation_type) THEN
                UPDATE SET 
                    t.exchange_rate              = s.exchange_rate,
                    t.is_originally_null         = s.is_originally_null,
                    t.moving_avg_7d              = s.moving_avg_7d,
                    t.transformed_at             = s.transformed_at,
                    t.moving_avg_30d             = s.moving_avg_30d,
                    t.rate_yesterday             = s.rate_yesterday,
                    t.variacion_pj_diaria_exrate = s.variacion_pj_diaria_exrate,
                    t.dev_std_30d                = s.dev_std_30d,
                    t.racha_rellenado            = s.racha_rellenado,
                    t.is_active                  = s.is_active,
                    t.operation_type             = s.operation_type,
                    t.ingestion_timestamp        = s.ingestion_timestamp,
                    t.updated_at                 = s.updated_at
            WHEN NOT MATCHED THEN
                INSERT (
                    exchange_date, base_currency, target_currency, exchange_rate, 
                    is_originally_null, moving_avg_7d, transformed_at, moving_avg_30d, 
                    rate_yesterday, variacion_pj_diaria_exrate, dev_std_30d, racha_rellenado, 
                    is_active, operation_type, ingestion_timestamp, updated_at
                )
                VALUES (
                    s.exchange_date, s.base_currency, s.target_currency, s.exchange_rate, 
                    s.is_originally_null, s.moving_avg_7d, s.transformed_at, s.moving_avg_30d, 
                    s.rate_yesterday, s.variacion_pj_diaria_exrate, s.dev_std_30d, s.racha_rellenado, 
                    s.is_active, s.operation_type, s.ingestion_timestamp, s.updated_at
                );
        """
        spark.sql(merge_query)
        
    print("[ÉXITO] Capa Silver sincronizada correctamente en Apache Iceberg.")