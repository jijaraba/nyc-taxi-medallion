# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Ingesta Bronze: viajes de taxi amarillo
# MAGIC `raw/nyc_taxi/yellow/yellow_tripdata_YYYY-MM.parquet` → `bronze.yellow_trips`
# MAGIC
# MAGIC - **Carga incremental por mes**: cada mes de `p_months` reemplaza solo su partición
# MAGIC   (`replaceWhere`), así re-ejecutar no duplica y agregar un mes nuevo no reprocesa los anteriores.
# MAGIC - **Schema drift**: cada archivo se lleva al esquema destino aunque cambien nombres o tipos.
# MAGIC - **Reglas duras** (lo que no cumple va a `bronze.quarantine`):
# MAGIC   fechas y zonas obligatorias · llegada posterior a la salida · fecha de recogida dentro del mes
# MAGIC   del archivo · distancia, tarifa y total no negativos · zonas entre 1 y 265.

# COMMAND ----------

# MAGIC %run ./utils/common

# COMMAND ----------

catalog = get_param("p_catalog", "nyc_taxi_dev")
storage_account = get_param("p_storage_account", "<storage_account>")
run_id = get_param("p_run_id", "manual")
months = parse_months(get_param("p_months", "2024-12,2025-01,2025-02"))

TARGET = StructType([
    StructField("vendor_id", IntegerType()), StructField("pickup_datetime", TimestampType()),
    StructField("dropoff_datetime", TimestampType()), StructField("passenger_count", IntegerType()),
    StructField("trip_distance", DoubleType()), StructField("ratecode_id", IntegerType()),
    StructField("store_and_fwd_flag", StringType()), StructField("pu_location_id", IntegerType()),
    StructField("do_location_id", IntegerType()), StructField("payment_type", IntegerType()),
    StructField("fare_amount", DoubleType()), StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()), StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()), StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()), StructField("congestion_surcharge", DoubleType()),
    StructField("airport_fee", DoubleType()), StructField("cbd_congestion_fee", DoubleType()),
])

# nombre en el archivo (en minúsculas) -> nombre estándar
RENAME = {
    "vendorid": "vendor_id", "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime", "passenger_count": "passenger_count",
    "trip_distance": "trip_distance", "ratecodeid": "ratecode_id", "store_and_fwd_flag": "store_and_fwd_flag",
    "pulocationid": "pu_location_id", "dolocationid": "do_location_id", "payment_type": "payment_type",
    "fare_amount": "fare_amount", "extra": "extra", "mta_tax": "mta_tax", "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount", "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount", "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "airport_fee", "cbd_congestion_fee": "cbd_congestion_fee",
}

# COMMAND ----------

summary = {}
for ym in months:
    path = f"{raw_path(storage_account, 'yellow')}yellow_tripdata_{ym}.parquet"
    df_raw = read_parquet_with_drift(path, TARGET, RENAME).withColumn("source_year_month", F.lit(ym))

    hard_rules = (
        (F.col("dropoff_datetime") > F.col("pickup_datetime"))
        & (F.date_format("pickup_datetime", "yyyy-MM") == F.lit(ym))
        & (F.col("trip_distance") >= 0)
        & (F.col("fare_amount") >= 0)
        & (F.col("total_amount") >= 0)
        & F.col("pu_location_id").between(1, 265)
        & F.col("do_location_id").between(1, 265)
    )
    valid, invalid = split_valid_invalid(
        df_raw, ["pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id"], hard_rules
    )

    quarantined = send_to_quarantine(invalid, catalog, f"yellow_trips_{ym}", run_id)
    replace_partitions(
        add_audit_columns(valid, "tlc_parquet", run_id),
        f"{catalog}.bronze.yellow_trips", "source_year_month", [ym],
    )

    metrics = {
        "rows_read": df_raw.count(),
        "rows_valid": spark.table(f"{catalog}.bronze.yellow_trips").filter(F.col("source_year_month") == ym).count(),
        "rows_quarantined": quarantined,
        "null_passenger_count": valid.filter(F.col("passenger_count").isNull()).count(),
    }
    log_metrics(catalog, run_id, "bronze", f"yellow_trips_{ym}", metrics)
    summary[ym] = metrics

dbutils.notebook.exit(json.dumps(summary))
