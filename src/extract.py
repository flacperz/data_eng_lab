import os
import requests
import time
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType


class FrankfurterClient:
    def __init__(self):
        """
        Cliente para consumir la API de Frankfurter con configuraciones del .env
        """
        self.base_url = os.getenv("API_BASE_URL", "https://api.frankfurter.dev/v1")
        self.base_currency = os.getenv("BASE_CURRENCY", "USD")
        self.targets = os.getenv("TARGET_CURRENCIES", "MXN,EUR,AUD,CAD")

    def get_currencies(self):        
        url = f"{self.base_url}/currencies"
        print("url", url)

        for attempt in range(3):
            try:
                print(f">>> [API] Consultando monedas disponibles {url} (Intento {attempt+1})...")
                response = requests.get(url, timeout=15)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                # El tiempo de espera aumenta al doble en cada intento (1s, 2s, 4s)
                wait = 2 ** attempt
                print(f"!!! [API ERROR] {e}. Reintentando en {wait}s...")
                time.sleep(wait)
        
        print("!!! [API ERROR] Se agotaron los reintentos.")
        return None

    def _save_log_extract(self, spark: SparkSession, log_data: dict):

        """Método auxiliar para registrar la auditoría en la tabla de control de Iceberg."""
        try:
            # Convertimos el diccionario plano a un DataFrame de Spark de una sola fila
            df_log = spark.createDataFrame([log_data])
                        
            columnas_ordenadas = [
                "fecha_ejecucion", 
                "fecha_inicio_consultado", 
                "fecha_fin_consultado", 
                "resultado", 
                "status_code"
            ]
            df_log_ordenado = df_log.select(*columnas_ordenadas)
            
            table_path = "local.db.control_extraccion_log"
            
            if not spark.catalog.tableExists(table_path):
                df_log_ordenado.writeTo(table_path).create()
            else:
                # Usamos append porque el log de auditoría es un histórico acumulativo (no lleva merge)
                df_log_ordenado.writeTo(table_path).append()
            print(f"[CONTROL] Registro de auditoría guardado en {table_path}")
        except Exception as err:
            print(f"[ALERTA] No se pudo guardar el log de control en Iceberg: {err}")


    def fetch_range(self, spark:SparkSession, start_date, end_date):
        """
        Consume la API para un rango de fechas.
        Punto Extra: Implementa reintentos con Backoff Exponencial.
        """
        # self.base_url = "https://api.frankfurter.totalmentefalsa_inexistente.xyz"
        url = f"{self.base_url}/{start_date}..{end_date}"
        params = {
            "base": self.base_currency,
            "symbols": self.targets
        }
        log_extract = {}

        log_extract["fecha_inicio_consultado"] = start_date
        log_extract["fecha_fin_consultado"] = end_date

        for attempt in range(3):
            
            try:
                print(f">>> [API] Extrayendo datos de {url} (Intento {attempt+1})...")
                response = requests.get(url, params=params, timeout=15)

                # Guardamos el código de antemano, ya que la respuesta sí llegó del servidor
                log_extract["status_code"] = str(response.status_code)

                response.raise_for_status()                
                
                # log_extract["status_code"] = response.status_code
                log_extract["fecha_ejecucion"] = datetime.now().isoformat()
                log_extract["resultado"] = "SUCCESS_INTENTO_" + str(attempt+1)
                
                # enviar a guardar en tabla log aqui
                self._save_log_extract(spark, log_extract)

                return response.json()
            except requests.exceptions.HTTPError as http_err:
                # status_code = e.response.status_code if e.response else "No Response"
                # log_extract["status_code"] = status_code
                log_extract["fecha_ejecucion"] = datetime.now().isoformat()                
                log_extract["resultado"] = "FAIL_INTENTO_" + str(attempt+1)

                if http_err.response is not None:
                    log_extract["status_code"] = str(http_err.response.status_code)
                else:
                    log_extract["status_code"] = "No Response"  

                # El tiempo de espera aumenta al doble en cada intento (1s, 2s, 4s)
                wait = 2 ** attempt
                print(f"!!! [API ERROR] {http_err}. Reintentando en {wait}s...")

                if attempt == 2:
                    log_extract["resultado"] = "FAIL_INTENTOS_AGOTADOS"
                    self._save_log_extract(spark, log_extract)

                time.sleep(wait)
            except requests.exceptions.RequestException as net_err:
                # Captura caídas de red, timeouts, errores de DNS (aquí NO hay respuesta del servidor)
                log_extract["status_code"] = "No Response / Network Error"
                log_extract["resultado"] = f"NETWORK_FAILED_ATTEMPT_{attempt+1}"
                log_extract["fecha_ejecucion"] = datetime.now().isoformat()

                wait = 2 ** attempt
                print(f"!!! [NETWORK ERROR] {net_err}. Reintentando en {wait}s...")
                
                if attempt == 2:
                    log_extract["resultado"] = "CRITICAL_NETWORK_TIMEOUT"
                    self._save_log_extract(spark, log_extract)
                    
                time.sleep(wait)
            finally:                
                print(f">>> [API LOG] {log_extract}")
                
        
        print("!!! [API ERROR] Se agotaron los reintentos.")
        return None

def raw_to_spark(spark, data):
    """
    Convierte el JSON crudo en un DataFrame de Spark (Capa Bronce).
    Aplica 'Schema Enforcement' para garantizar la calidad desde el inicio.
    """
    if not data or 'rates' not in data:
        print("!!! [RAW ERROR] No se encontraron tasas de cambio en la respuesta.")
        return None

    rows = []
    # Estructura del JSON: {'rates': {'YYYY-MM-DD': {'CURR': VAL}}}
    for date_str, rates in data['rates'].items():
        # Creamos la base de la fila con fecha y moneda base
        row = {
            "date": datetime.strptime(date_str, '%Y-%m-%d').date(),
            "base": data.get('base', 'USD')
        }
        # Añadimos dinámicamente las monedas configuradas
        for curr, val in rates.items():
            row[curr] = float(val)
        rows.append(row)

    # Definición explícita del esquema (Tipado fuerte)
    fields = [
        StructField("date", DateType(), False),
        StructField("base", StringType(), False)
    ]
    
    # Añadimos campos para cada moneda objetivo
    for curr in os.getenv("TARGET_CURRENCIES", "MXN,EUR,BRL,COP").split(','):
        fields.append(StructField(curr.strip(), DoubleType(), True))
        
    schema = StructType(fields)
    
    # Creamos el DataFrame
    return spark.createDataFrame(rows, schema)

import os
from pyspark.sql import SparkSession

def save_to_bronze(spark: SparkSession, df_new: DataFrame, table_name: str = "tipos_cambio_raw"):
    """
    Persiste el DataFrame en la capa Bronze usando Apache Iceberg.
    Soporta carga histórica (creación) e incremental (Merge/Upsert).
    """
    catalog_name = os.getenv("CATALOG_NAME", "local")
    database_name = "db"
    full_table_path = f"{catalog_name}.{database_name}.{table_name}"
    
    # 1. Verificar si la tabla ya existe en el catálogo de Iceberg
    table_exists = spark.catalog.tableExists(full_table_path)
    
    if not table_exists:
        # --- ESCENARIO A: Carga Histórica (Creación de Tabla) ---
        print(f">>> [HISTÓRICO] Creando tabla Iceberg por primera vez en: {full_table_path}")
        
        # Registramos el DataFrame inicial como la tabla base
        df_new.writeTo(full_table_path) \
            .tableProperty("write.format.default", "parquet") \
            .create()
            
    else:
        # --- ESCENARIO B: Carga Incremental (Merge / Upsert) ---
        print(f">>> [INCREMENTAL] Aplicando MERGE INTO en tabla: {full_table_path}")
        
        # Registramos el DataFrame nuevo como una vista temporal para poder usar Spark SQL
        df_new.createOrReplaceTempView("incremental_view")
        
        # El MERGE asegura que si la fecha ya existe, se actualice (evita duplicados), 
        # y si es nueva, se inserte.
        merge_query = f"""
            MERGE INTO {full_table_path} target
            USING incremental_view source
            ON target.date = source.date
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """
        spark.sql(merge_query)
        
    print(f">>> [ÉXITO] Datos persistidos correctamente en {full_table_path}")
