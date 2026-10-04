# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Preparación del ambiente (en cada ejecución del workflow)
# MAGIC Idempotente y 100 % Python:
# MAGIC - Catálogo y esquemas con **Databricks SDK**.
# MAGIC - Tablas Bronze y de auditoría con el builder de Delta (`DeltaTable.createIfNotExists`).
# MAGIC
# MAGIC **Prerrequisito:** storage credential y external locations de `PrepAmb/01_prep_ambiente.py`.

# COMMAND ----------

# MAGIC %run ./utils/common

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

catalog = get_param("p_catalog", "nyc_taxi_dev")
storage_account = get_param("p_storage_account", "<storage_account>")


def abfss(container: str) -> str:
    return f"abfss://{container}@{storage_account}.dfs.core.windows.net/{catalog}"


def create_if_missing(label: str, fn, **kwargs):
    try:
        fn(**kwargs)
        print("Creado:   ", label)
    except Exception as e:
        if "already exists" in str(e).lower() or "ALREADY_EXISTS" in str(e):
            print("Existente:", label)
        else:
            raise

# COMMAND ----------

create_if_missing(f"catálogo {catalog}", w.catalogs.create,
                  name=catalog, storage_root=abfss("catalog"), comment="NYC Taxi - arquitectura medallion")
for layer in ["bronze", "silver", "gold"]:
    create_if_missing(f"esquema {catalog}.{layer}", w.schemas.create,
                      name=layer, catalog_name=catalog, storage_root=abfss(layer))
create_if_missing(f"esquema {catalog}.audit", w.schemas.create,
                  name="audit", catalog_name=catalog, comment="Logs de calidad y ejecución")

# COMMAND ----------


def fields(*cols):
    return [StructField(name, dtype, True) for name, dtype in cols]


AUDIT = fields(("source_file", StringType()), ("ingestion_ts", TimestampType()),
               ("data_source", StringType()), ("run_id", StringType()))

TRIPS = fields(
    ("vendor_id", IntegerType()), ("pickup_datetime", TimestampType()), ("dropoff_datetime", TimestampType()),
    ("passenger_count", IntegerType()), ("trip_distance", DoubleType()), ("ratecode_id", IntegerType()),
    ("store_and_fwd_flag", StringType()), ("pu_location_id", IntegerType()), ("do_location_id", IntegerType()),
    ("payment_type", IntegerType()), ("fare_amount", DoubleType()), ("extra", DoubleType()),
    ("mta_tax", DoubleType()), ("tip_amount", DoubleType()), ("tolls_amount", DoubleType()),
    ("improvement_surcharge", DoubleType()), ("total_amount", DoubleType()),
    ("congestion_surcharge", DoubleType()), ("airport_fee", DoubleType()), ("cbd_congestion_fee", DoubleType()),
    ("source_year_month", StringType()),
)

# (tabla, columnas, comentario, columna de partición)
tables = [
    ("bronze.yellow_trips", TRIPS + AUDIT, "Viajes de taxi amarillo (1 fila por viaje)", "source_year_month"),
    ("bronze.taxi_zones",
     fields(("location_id", IntegerType()), ("borough", StringType()), ("zone", StringType()),
            ("service_zone", StringType())) + AUDIT, "Zonas de taxi TLC", None),
    ("bronze.ref_codes",
     fields(("code_type", StringType()), ("code", IntegerType()), ("description", StringType())) + AUDIT,
     "Códigos del diccionario de datos TLC", None),
    ("bronze.weather_daily",
     fields(("weather_date", DateType()), ("temperature_max_c", DoubleType()), ("temperature_min_c", DoubleType()),
            ("precipitation_mm", DoubleType()), ("snowfall_cm", DoubleType()),
            ("wind_speed_max_kmh", DoubleType())) + AUDIT, "Clima diario NYC (API Open-Meteo)", None),
    ("bronze.quarantine",
     fields(("entity", StringType()), ("raw_record", StringType()), ("source_file", StringType()),
            ("run_id", StringType()), ("quarantine_ts", TimestampType())),
     "Registros rechazados por reglas de calidad", "entity"),
    ("audit.data_quality_log",
     fields(("run_id", StringType()), ("layer", StringType()), ("entity", StringType()),
            ("metric", StringType()), ("value", LongType()), ("log_ts", TimestampType())),
     "Métricas de calidad por ejecución", None),
]

for name, cols, comment, partition in tables:
    builder = (DeltaTable.createIfNotExists(spark)
               .tableName(f"{catalog}.{name}")
               .addColumns(StructType(cols))
               .comment(comment))
    if partition:
        builder = builder.partitionedBy(partition)
    builder.execute()
    print("Tabla lista:", f"{catalog}.{name}")

dbutils.notebook.exit("ambiente listo")
