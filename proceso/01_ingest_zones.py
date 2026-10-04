# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Ingesta Bronze: zonas de taxi
# MAGIC `raw/nyc_taxi/taxi_zones/taxi_zone_lookup.csv` → `bronze.taxi_zones`
# MAGIC
# MAGIC Simula un **maestro de zonas en Azure SQL**: si Data Factory lo copia en Parquet,
# MAGIC usar `p_source_format = parquet`.

# COMMAND ----------

# MAGIC %run ./utils/common

# COMMAND ----------

catalog = get_param("p_catalog", "nyc_taxi_dev")
storage_account = get_param("p_storage_account", "<storage_account>")
run_id = get_param("p_run_id", "manual")
source_format = get_param("p_source_format", "csv")

# Con esquema explícito los nombres del encabezado (LocationID, Borough...) se reemplazan por estos
schema = StructType([
    StructField("location_id", IntegerType()), StructField("borough", StringType()),
    StructField("zone", StringType()), StructField("service_zone", StringType()),
])

if source_format == "parquet":
    df_raw = read_parquet_with_drift(
        raw_path(storage_account, "taxi_zones"), schema,
        {"locationid": "location_id", "borough": "borough", "zone": "zone", "service_zone": "service_zone"},
    )
else:
    df_raw = read_raw(raw_path(storage_account, "taxi_zones"), schema, "csv")

valid, invalid = split_valid_invalid(df_raw, ["location_id"], F.col("location_id").between(1, 265))
quarantined = send_to_quarantine(invalid, catalog, "taxi_zones", run_id)

data_source = "azure_sql_via_adf" if source_format == "parquet" else "tlc_csv"
upsert_delta(add_audit_columns(valid, data_source, run_id), f"{catalog}.bronze.taxi_zones", ["location_id"])

metrics = {"rows_read": df_raw.count(), "rows_valid": valid.count(), "rows_quarantined": quarantined}
log_metrics(catalog, run_id, "bronze", "taxi_zones", metrics)
dbutils.notebook.exit(json.dumps(metrics))
