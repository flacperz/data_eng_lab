# Konfio Data Engineering Challenge - Lakehouse Pipeline

Este repositorio contiene la solución a la prueba técnica para la posición de Data Engineer en **Konfio**. El proyecto implementa un pipeline de datos end-to-end (Batch e Incremental/CDC) utilizando **PySpark**, **Apache Iceberg**.

La arquitectura sigue el patrón **Medallion (Bronze -> Silver -> Gold)** para procesar de forma robusta e idempotente el histórico y las actualizaciones de tipos de cambio extraídos de la API Frankfurter.

---

## Arquitectura del Lakehouse

El flujo de datos se divide en tres capas lógicas autocontenidas:

1. **Capa Bronze (Raw Data):** Ingesta cruda de los eventos de la API y persistencia en formato JSON/DataFrame, sirviendo como el histórico inmutable de auditoría.
2. **Capa Silver (Enriched Data):** Aplicación de reglas de calidad, manejo de nulos por fines de semana/festivos mediante ventanas analíticas (`Window`), y detección de cambios de estado (**CDC analítico**). Mapea inserciones, actualizaciones y borrados lógicos (`is_active`).
3. **Capa Gold (Analytical Model):** Modelo dimensional en estrella que expone tablas de dimensiones y la tabla de hechos unificada (`fact_exchange_rates`) optimizada para consultas analíticas de riesgo crediticio y finanzas.

---

## Tecnologías Utilizadas

* **Python 3.10** & **PySpark (Spark 3.x)**
* **Apache Iceberg** (Formato de tabla abierto para almacenamiento ACID de alto rendimiento)
* **Docker & Docker Compose** (Para la containerización completa del entorno local)
* **Jupyter Notebook** (Para análisis exploratorio y auditoría)

---

## Estructura del Proyecto

```text
├── app/
│   ├── events/
│   │   └── *.json                # Creación de eventos derivados del CDC como archivos .json
│   ├── src/
│   │   ├── agregaciones.py       # Tablas de agregaciones mensuales y anomalias
│   │   ├── data_quality.py       # Tablas de cobertura de fechas y estadisticas del lote de carga
│   │   ├── extract.py            # Extracción de la API y persistencia en Bronze
│   │   ├── orquestador.py        # Orquesta la ejecución de cada fase desde raw -> silver -> gold
│   │   ├── process_fact_dims.py  # Construcción del modelo dimensional (Gold)
│   │   └── transform.py          # Lógica del CDC, control de rachas y capa Silver
│   │   └── utils.py              # Utilerias para reutilizar
├────── tests/
│   │   ├── test_transform.py     # Pruebas de CDC principalmente
├── docker-compose.yml            # Orquestación de contenedores (Spark, Iceberg)
├── Dockerfile                    # Orquestación de contenedores (Spark, Iceberg)
├── main.py                       # Orquestador principal del pipeline completo, punto de entrada para la ejecución del pipeline
└── requirements.txt              # Dependencias de Python


NOTA: Se sacó main.py de src para tener plenamente identificado el punto de entrada al pipeline.
```
---

## Modelo dimensional de datos

Se toma en cuenta y se crea lo que se solicita, las tablas creadas son:
- **fact_exchange_rates**
- **dim_time**
- **dim_currencies**


---

## Instalación

El flujo de datos se divide en tres capas lógicas autocontenidas:

1. Clonar repositorio desde la ruta proporcionada.
2. Renombar archivo .env.example como .env para que el servicio se inicie y cargue variables de entorno.
3. Entrar a carpeta base y ejectuar **docker compose up --build**


## Resultados Esperados

El flujo debe pasar por al menos 2 ejecuciones para completar la información al día.

    1.- Primer ejecución. Recupera el histórico solicitado explicitamente.
        START_DATE=2024-01-01
        END_DATE=2024-06-30
    2.- Segunda ejecución y posteriores.
        Se calcula el siguiente lote de fechas para el incremental, se considera desde la maxima fecha cargada hasta fecha actual.
        Las siguientes ejecuciones serán solo para tomar de la API el último día. (Se asume que se ejecuta diario). En caso de que no se ejecute algun dia,
        se tomará la ultima fecha cargada y desde ahi se consultará el API hasta dia actual.

---
## Notebook para análisis exploratorio

Se incluye notebook: analisis_exploratorio.ipynb con el cual se puede hacer revisión de esquemas, conteos y filtros sobre la data cargada.