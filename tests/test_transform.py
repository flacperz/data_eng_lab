import sys
import os
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Aseguramos que Python encuentre el módulo src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.transform import compute_cdc
from src.utils import getSparkSession


def run_cdc_tests():
    spark = getSparkSession()
    spark.sparkContext.setLogLevel("ERROR")
    
    print("\n[TEST] Iniciando pruebas unitarias para el módulo CDC...\n")
    
    # -------------------------------------------------------------------------
    # ESCENARIO 1: Primera corrida (Tabla Silver Inexistente / Vacía)
    # -------------------------------------------------------------------------
    print("Ejecutando Escenario 1: Carga Inicial (Silver vacía)...")
    
    # Creamos un lote simulado que viene de Bronze enriquecido
    lote_inicial_data = [
        (date(2026, 5, 15), "EUR", "USD", 1.0850, 0.0, 1.0850, 1.0850, 0.0),
        (date(2026, 5, 15), "EUR", "MXN", 18.5000, 0.0, 18.5000, 18.5000, 0.0)
    ]
    columnas = ["exchange_date", "base_currency", "target_currency", "exchange_rate", 
                "daily_variation_pct", "moving_avg_7d", "moving_avg_30d", "moving_stddev_30d"]
    
    df_lote_inicial = spark.createDataFrame(lote_inicial_data, schema=columnas)
    
    # Al ser la primera corrida, el histórico se pasa como None
    df_resultado_1 = compute_cdc(df_lote_inicial, df_silver_actual=None)
    
    # Validaciones estratégicas
    assert df_resultado_1.count() == 2, "Error: Deberían procesarse 2 registros."
    
    # Verificamos que todos hayan sido marcados como INSERT
    operaciones_1 = df_resultado_1.select("operation_type").distinct().collect()
    assert len(operaciones_1) == 1 and operaciones_1[0]["operation_type"] == "INSERT", "Error: El operation_type debe ser INSERT."
    print("Escenario 1 Exitoso: Todo el lote inicial se clasificó como INSERT.\n")
    
    # -------------------------------------------------------------------------
    # ESCENARIO 2: Corrida Incremental (Detección de Cambios y Nuevos)
    # -------------------------------------------------------------------------
    print("Ejecutando Escenario 2: Lote Delta Incremental...")
    
    # Simulamos el histórico de Silver añadiéndole las columnas de auditoría que crearía el merge
    df_silver_historico = df_resultado_1
    
    # Creamos un nuevo lote delta que trae:
    # 1. El registro de USD modificado (la tasa bajó de 1.0850 a 1.0710) -> Debe ser UPDATE
    # 2. El registro de MXN idéntico (18.5000) -> Debe ser ignorado (NO_CHANGE)
    # 3. Un par completamente nuevo (EUR/GBP) para la misma fecha -> Debe ser INSERT
    lote_delta_data = [
        (date(2026, 5, 15), "EUR", "USD", 1.0710, -0.012, 1.0780, 1.0780, 0.005), # Editado
        (date(2026, 5, 15), "EUR", "MXN", 18.5000, 0.0, 18.5000, 18.5000, 0.0),    # Idéntico
        (date(2026, 5, 15), "EUR", "GBP", 0.8500, 0.0, 0.8500, 0.8500, 0.0)        # Nuevo
    ]
    df_lote_delta = spark.createDataFrame(lote_delta_data, schema=columnas)
    
    # Ejecutamos el CDC pasando el histórico simulado
    df_resultado_2 = compute_cdc(df_lote_delta, df_silver_historico)
    
    # Validaciones del delta
    # De los 3 registros, el MXN idéntico debe ser descartado en el output final para no duplicar datos
    assert df_resultado_2.count() == 2, f"Error: Deberían quedar 2 registros modificados, quedaron {df_resultado_2.count()}."
    
    # Extraemos las acciones detectadas para verificar la precisión analítica
    resultados = df_resultado_2.select("target_currency", "operation_type").collect()
    mapa_resultados = {row["target_currency"]: row["operation_type"] for row in resultados}
    
    assert mapa_resultados.get("USD") == "UPDATE", f"Error: El par USD debió marcarse como UPDATE, quedó {mapa_resultados.get('USD')}."
    assert mapa_resultados.get("GBP") == "INSERT", f"Error: El par GBP debió marcarse como INSERT, quedó {mapa_resultados.get('GBP')}."
    assert "MXN" not in mapa_resultados, "Error: El registro idéntico de MXN debió filtrarse (NO_CHANGE)."
    
    print("Escenario 2 Exitoso: Se detectó 1 UPDATE (USD), 1 INSERT (GBP) y se ignoró el registro duplicado (MXN).")
    print("\n[ÉXITO TOTAL] Todas las pruebas del motor de CDC han pasado la validación de consistencia.\n")

if __name__ == "__main__":
    run_cdc_tests()