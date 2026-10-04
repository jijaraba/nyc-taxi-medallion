# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Ingesta Bronze: códigos de referencia (JSON)
# MAGIC `raw/nyc_taxi/reference/ref_codes.json` → `bronze.ref_codes`
# MAGIC
# MAGIC Traduce los códigos del diccionario de datos de la TLC (proveedor, tarifa, forma de pago).
# MAGIC Formato JSON Lines: una fuente **semiestructurada** adicional.

# COMMAND ----------

# MAGIC %run ./utils/common

# COMMAND ----------

catalog = get_param("p_catalog", "nyc_taxi_dev")
storage_account = get_param("p_storage_account", "<storage_account>")
run_id = get_param("p_run_id", "manual")

schema = StructType([
    StructField("code_type", StringType()), StructField("code", IntegerType()),
    StructField("description", StringType()),
])

df_raw = read_raw(raw_path(storage_account, "reference"), schema, "json")
rule = F.col("code_type").isin("vendor", "ratecode", "payment_type")
valid, invalid = split_valid_invalid(df_raw, ["code_type", "code", "description"], rule)

quarantined = send_to_quarantine(invalid, catalog, "ref_codes", run_id)
upsert_delta(add_audit_columns(valid, "tlc_data_dictionary_json", run_id),
             f"{catalog}.bronze.ref_codes", ["code_type", "code"])

metrics = {"rows_read": df_raw.count(), "rows_valid": valid.count(), "rows_quarantined": quarantined}
log_metrics(catalog, run_id, "bronze", "ref_codes", metrics)
dbutils.notebook.exit(json.dumps(metrics))
