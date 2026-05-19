# 🚀 Konfio Data Engineering Challenge - Lakehouse Pipeline

Este repositorio contiene la solución a la prueba técnica para la posición de Data Engineer en **Konfio**. El proyecto implementa un pipeline de datos end-to-end (Batch e Incremental/CDC) utilizando **PySpark**, **Apache Iceberg**, y una arquitectura orientada a eventos con **Docker** y **Kafka**.

La arquitectura sigue el patrón **Medallion (Bronze -> Silver -> Gold)** para procesar de forma robusta e idempotente el histórico y las actualizaciones de tipos de cambio extraídos de la API Frankfurter.

---

## 🏗️ Arquitectura del Lakehouse

El flujo de datos se divide en tres capas lógicas autocontenidas:

1. **Capa Bronze (Raw Data):** Ingesta cruda de los eventos de la API y persistencia en formato JSON/DataFrame, sirviendo como el histórico inmutable de auditoría.
2. **Capa Silver (Enriched Data):** Aplicación de reglas de calidad, manejo de nulos por fines de semana/festivos mediante ventanas analíticas (`Window`), y detección de cambios de estado (**CDC analítico**). Mapea inserciones, actualizaciones y borrados lógicos (`is_active`).
3. **Capa Gold (Analytical Model):** Modelo dimensional en estrella que expone tablas de dimensiones y la tabla de hechos unificada (`fact_exchange_rates`) optimizada para consultas analíticas de riesgo crediticio y finanzas.

---

## 🛠️ Tecnologías Utilizadas

* **Python 3.10** & **PySpark (Spark 3.x)**
* **Apache Iceberg** (Formato de tabla abierto para almacenamiento ACID de alto rendimiento)
* **Apache Kafka & Zookeeper** (Streaming de eventos y Change Data Capture orientado a eventos)
* **Docker & Docker Compose** (Para la containerización completa del entorno local)
* **Jupyter Notebook** (Para análisis exploratorio y auditoría)

---

## 📂 Estructura del Proyecto

```text
├── app/
│   ├── config/
│   │   └── settings.py             # Parámetros globales y configuración del entorno
│   ├── database/
│   │   └── connection.py           # Inicialización y sesión del clúster Spark / Iceberg
│   ├── kafka_integration/
│   │   └── producer.py             # Publicación de eventos derivados del CDC en Kafka
│   ├── notebooks/
│   │   └── data_analysis.ipynb     # Jupyter Notebook con QA y EDA
│   ├── src/
│   │   ├── extract_bronze.py       # Extracción de la API y persistencia en Bronze
│   │   ├── process_fact_dims.py    # Construcción del modelo dimensional (Gold)
│   │   └── process_silver.py       # Lógica del CDC, control de rachas y capa Silver
│   ├── utils/
│   │   └── helpers.py              # Funciones auxiliares y manejo de fechas/logs
│   └── main.py                     # Orquestador principal del pipeline completo
├── docker-compose.yml              # Orquestación de contenedores (Spark, Iceberg, Kafka)
├── README.md                       # Documentación del proyecto
└── requirements.txt                # Dependencias de Python