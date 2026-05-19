import argparse
from src.orquestador import run_bronze_pipeline, run_silver_pipeline, run_gold_pipeline
from src.utils import validar_formato_fecha

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Carga de datos de tipos de cambio a la capa Bronze.")
    parser.add_argument("--start_date", type=validar_formato_fecha, help="Fecha de inicio para la extracción (formato YYYY-MM-DD)", default=None)
    parser.add_argument("--end_date", type=validar_formato_fecha, help="Fecha de fin para la extracción (formato YYYY-MM-DD)", default=None)
    args = parser.parse_args()

    fecha_inicio, fecha_fin, conteo_registros_procesados = run_bronze_pipeline(args.start_date, args.end_date)
    if conteo_registros_procesados is not None and conteo_registros_procesados > 0:
        run_silver_pipeline(fecha_inicio, fecha_fin)
        run_gold_pipeline(fecha_inicio, fecha_fin)
    else:
        print("No se procesaron registros en Bronze. Saltando etapas Silver y Gold.")