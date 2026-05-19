import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from datetime import datetime
import argparse

def getSparkSession(app_name: str = "KonfioApp") -> SparkSession:
    """
    Crea y retorna una instancia de SparkSession con configuraciones para Apache Iceberg.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{os.getenv('CATALOG_NAME')}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{os.getenv('CATALOG_NAME')}.type", "hadoop") \
        .config(f"spark.sql.catalog.{os.getenv('CATALOG_NAME')}.warehouse", os.getenv('WAREHOUSE_PATH')) \
        .getOrCreate()
    return spark

print("::::UTILS:::: ----> Spark e Iceberg listos en Jupyter")

def check_table_exists(spark: SparkSession, table_name: str) -> bool:
    """
    Verifica si una tabla existe en el catálogo de Iceberg.
    Ejemplo de table_name: 'local.db.raw_exchange_rates'
    """

    if len(table_name.split('.')) != 3:
        local_table_name = f"local.db.{table_name}"
    else:
        local_table_name = table_name
    
    try:
        return spark.catalog.tableExists(local_table_name)
    except Exception:
        return False

def get_max_date_loaded(spark: SparkSession, table_name: str) -> datetime.date:
    """
    Consulta la tabla de Iceberg para obtener la fecha máxima cargada.
    Esto es útil para determinar el rango de fechas a extraer en cargas incrementales.
    """
    if len(table_name.split('.')) != 3:
        local_table_name = f"local.db.{table_name}"
    else:
        local_table_name = table_name
        
    try:
        max_date = spark.table(local_table_name).agg({"date": "max"}).collect()[0][0]
        return max_date
    except Exception as e:
        print(f"!!! [ERROR] No se pudo obtener la fecha máxima de {local_table_name}: {e}")
        return None


def validar_formato_fecha(fecha_str):
    """
    Valida si el string recibido tiene el formato estricto YYYY-MM-DD.
    Si es correcto, devuelve el string. Si no, lanza un error de argparse.
    """
    # Si Airflow no mandó nada, permitimos que pase el None para activar la lógica automática
    if not fecha_str or fecha_str == "":
        return None
        
    try:
        # Intentamos parsear el formato estricto
        datetime.strptime(fecha_str, "%Y-%m-%d")
        return fecha_str
    except ValueError:
        msg = f"Formato de fecha inválido: '{fecha_str}'. Debe ser estrictamente YYYY-MM-DD."
        raise argparse.ArgumentTypeError(msg)


import os
import json
from datetime import datetime
from pyspark.sql import DataFrame

def emit_cdc_events(df_final_cdc: DataFrame, output_dir: str = "events"):
    """
    Sección 5: Emisión de Eventos a partir de los cambios detectados por el CDC.
    Genera archivos JSON individuales orientados a eventos para cada cambio.
    """
    print(f"[EVENTS] Generando salida orientada a eventos en carpeta: /{output_dir}...")
    
    # 1. Aseguramos que la carpeta de destino exista físicamente en el disco
    os.makedirs(output_dir, exist_ok=True)
    
    # 2. Nos traemos los datos del CDC a la memoria del Driver para serializar los JSON
    # collect() es seguro aquí porque el lote delta diario es pequeño y controlado
    records = df_final_cdc.collect()
    
    if not records:
        print("[EVENTS] No se detectaron cambios en el CDC para generar eventos.")
        return 0

    event_count = 0
    current_time_str = datetime.now().isoformat()

    for row in records:
        # Convertimos la fecha a string para que sea serializable en JSON
        fecha_str = row["exchange_date"].strftime("%Y-%m-%d")
        base = row["base_currency"]
        target = row["target_currency"]
        
        #Identificador Único de Entidad (Clave Compuesta)
        entity_id = f"{fecha_str.replace('-', '')}_{base}_{target}"
        
        # Mapeamos LOGICAL_DELETE a DELETE para cumplir estrictamente con el documento
        op_type = "DELETE" if row["operation_type"] == "LOGICAL_DELETE" else row["operation_type"]
        
        daily_var = row["variacion_pj_diaria_exrate"]
        moving_avg = row["moving_avg_30d"]
        rate_val = row["exchange_rate"]
        is_active = row["is_active"]
        
        # 3. Estructuramos el cuerpo del evento según la especificación exacta del documento
        event_payload = {
            "event_type": op_type,
            "event_timestamp": current_time_str,
            "entity_id": entity_id,
            "payload": {
                "exchange_date": fecha_str,
                "base_currency": base,
                "target_currency": target,
                "exchange_rate": float(rate_val) if rate_val is not None else None,
                "daily_variation_pct": float(daily_var) if daily_var is not None else None,
                "moving_avg_30d": float(moving_avg) if moving_avg is not None else None,
                "is_active": bool(is_active) if is_active is not None else None
            }
        }
        
        # 4. Nombre del archivo único para evitar colisiones: fecha_moneda_operacion_timestamp
        file_name = f"event_{entity_id}_{op_type.lower()}.json"
        file_path = os.path.join(output_dir, file_name)
        
        # 5. Escritura física en disco
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(event_payload, f, indent=4, ensure_ascii=False)
            
        event_count += 1

    print(f"[EVENTS] Emisión finalizada con éxito. Se generaron {event_count} archivos de eventos JSON.")
    return event_count