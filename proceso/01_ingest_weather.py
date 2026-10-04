# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Ingesta Bronze: clima diario (API REST)
# MAGIC 1. Llama a la API pública **Open-Meteo Historical Weather** (sin API key) para el rango de `p_months`.
# MAGIC 2. Guarda la respuesta JSON tal cual en **raw** (`raw/nyc_taxi/weather/`), vía external location.
# MAGIC 3. Lee el JSON de raw, convierte los arreglos diarios en filas (`arrays_zip` + `explode`) y
# MAGIC    hace MERGE en `bronze.weather_daily`.
# MAGIC
# MAGIC Permite responder: ¿cómo afectan la lluvia y la nieve a la demanda y a las propinas?

# COMMAND ----------

# MAGIC %run ./utils/common

# COMMAND ----------

import calendar
import time

import requests

catalog = get_param("p_catalog", "nyc_taxi_dev")
storage_account = get_param("p_storage_account", "<storage_account>")
run_id = get_param("p_run_id", "manual")
months = parse_months(get_param("p_months", "2024-12,2025-01,2025-02"))

first_year, first_month = map(int, months[0].split("-"))
last_year, last_month = map(int, months[-1].split("-"))
start_date = f"{first_year:04d}-{first_month:02d}-01"
end_date = f"{last_year:04d}-{last_month:02d}-{calendar.monthrange(last_year, last_month)[1]:02d}"

DAILY_VARS = ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "snowfall_sum", "wind_speed_10m_max"]
params = {
    "latitude": 40.7128, "longitude": -74.0060,  # Manhattan
    "start_date": start_date, "end_date": end_date,
    "daily": ",".join(DAILY_VARS),
    "timezone": "America/New_York",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-2. API → raw

# COMMAND ----------

response = None
for attempt in range(1, 4):
    try:
        response = requests.get("https://archive-api.open-meteo.com/v1/archive", params=params, timeout=60)
        response.raise_for_status()
        break
    except requests.RequestException as e:
        print(f"Intento {attempt} falló: {e}")
        if attempt == 3:
            raise
        time.sleep(10 * attempt)

landing_file = f"{raw_path(storage_account, 'weather')}weather_{start_date}_{end_date}.json"
dbutils.fs.put(landing_file, response.text, overwrite=True)
print("Guardado en raw:", landing_file)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. raw → bronze

# COMMAND ----------

raw_json = (spark.read.option("multiLine", "true").json(landing_file)
            .withColumn("source_file", F.col("_metadata.file_path")))

daily = (
    raw_json.select("source_file", "daily.*")
    .select("source_file", F.explode(F.arrays_zip("time", *DAILY_VARS)).alias("d"))
    .select(
        F.to_date("d.time").alias("weather_date"),
        F.col("d.temperature_2m_max").cast("double").alias("temperature_max_c"),
        F.col("d.temperature_2m_min").cast("double").alias("temperature_min_c"),
        F.col("d.precipitation_sum").cast("double").alias("precipitation_mm"),
        F.col("d.snowfall_sum").cast("double").alias("snowfall_cm"),
        F.col("d.wind_speed_10m_max").cast("double").alias("wind_speed_max_kmh"),
        "source_file",
    )
    .withColumn("_corrupt_record", F.lit(None).cast("string"))
)

valid, invalid = split_valid_invalid(daily, ["weather_date"])
quarantined = send_to_quarantine(invalid, catalog, "weather_daily", run_id)
upsert_delta(add_audit_columns(valid, "open_meteo_api", run_id), f"{catalog}.bronze.weather_daily", ["weather_date"])

metrics = {"days_received": daily.count(), "rows_valid": valid.count(), "rows_quarantined": quarantined}
log_metrics(catalog, run_id, "bronze", "weather_daily", metrics)
dbutils.notebook.exit(json.dumps(metrics))
