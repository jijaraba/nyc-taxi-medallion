# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: dimensiones
# MAGIC - `silver.dim_zone`: zonas con nombres normalizados y banderas (aeropuerto, zona amarilla, desconocida).
# MAGIC - `silver.dim_reference`: descripciones en español de los códigos TLC.
# MAGIC - `silver.dim_date`: calendario **generado con PySpark** para el rango de `p_months`,
# MAGIC   enriquecido con el clima del día.

# COMMAND ----------

# MAGIC %run ./utils/common

# COMMAND ----------

import calendar

catalog = get_param("p_catalog", "nyc_taxi_dev")
run_id = get_param("p_run_id", "manual")
months = parse_months(get_param("p_months", "2024-12,2025-01,2025-02"))

UNKNOWN = "Desconocido"


def clean_label(col_name: str) -> Column:
    c = F.trim(F.col(col_name))
    return F.when(c.isNull() | c.isin("N/A", "NA", "Unknown", ""), F.lit(UNKNOWN)).otherwise(c)

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_zone

# COMMAND ----------

dim_zone = (
    spark.table(f"{catalog}.bronze.taxi_zones")
    .select(
        "location_id",
        clean_label("borough").alias("borough"),
        clean_label("zone").alias("zone"),
        clean_label("service_zone").alias("service_zone"),
    )
    .withColumn("is_airport", F.col("service_zone").isin("Airports", "EWR"))
    .withColumn("is_yellow_zone", F.col("service_zone") == "Yellow Zone")
    .withColumn("is_unknown", (F.col("borough") == UNKNOWN) | F.col("location_id").isin(264, 265))
    .withColumn("updated_ts", F.current_timestamp())
)
upsert_delta(dim_zone, f"{catalog}.silver.dim_zone", ["location_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_reference

# COMMAND ----------

dim_reference = (
    spark.table(f"{catalog}.bronze.ref_codes")
    .select("code_type", "code", F.trim("description").alias("description"))
    .withColumn("updated_ts", F.current_timestamp())
)
upsert_delta(dim_reference, f"{catalog}.silver.dim_reference", ["code_type", "code"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_date + clima

# COMMAND ----------

first_year, first_month = map(int, months[0].split("-"))
last_year, last_month = map(int, months[-1].split("-"))
start = f"{first_year:04d}-{first_month:02d}-01"
end = f"{last_year:04d}-{last_month:02d}-{calendar.monthrange(last_year, last_month)[1]:02d}"

DAY_NAMES = {1: "domingo", 2: "lunes", 3: "martes", 4: "miércoles", 5: "jueves", 6: "viernes", 7: "sábado"}
MONTH_NAMES = {1: "enero", 2: "febrero", 3: "marzo", 4: "abril", 5: "mayo", 6: "junio", 7: "julio",
               8: "agosto", 9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"}
day_map = F.create_map(*[F.lit(x) for kv in DAY_NAMES.items() for x in kv])
month_map = F.create_map(*[F.lit(x) for kv in MONTH_NAMES.items() for x in kv])

calendar_df = (
    spark.range(1)
    .select(F.explode(F.sequence(F.to_date(F.lit(start)), F.to_date(F.lit(end)), F.expr("interval 1 day"))).alias("date"))
    .withColumns({
        "date_key": F.date_format("date", "yyyyMMdd").cast("int"),
        "year": F.year("date"),
        "quarter": F.quarter("date"),
        "month": F.month("date"),
        "year_month": F.date_format("date", "yyyy-MM"),
        "day": F.dayofmonth("date"),
        "day_of_week": F.dayofweek("date"),
    })
    .withColumn("day_name", day_map[F.col("day_of_week")])
    .withColumn("month_name", month_map[F.col("month")])
    .withColumn("is_weekend", F.col("day_of_week").isin(1, 7))
)

weather = spark.table(f"{catalog}.bronze.weather_daily").select(
    F.col("weather_date").alias("date"), "temperature_max_c", "temperature_min_c",
    "precipitation_mm", "snowfall_cm", "wind_speed_max_kmh",
)

dim_date = (
    calendar_df.join(weather, "date", "left")
    .withColumn(
        "weather_condition",
        F.when(F.col("snowfall_cm") > 0, "nieve")
        .when(F.col("precipitation_mm") >= 5, "lluvia fuerte")
        .when(F.col("precipitation_mm") >= 0.5, "lluvia")
        .when(F.col("precipitation_mm").isNotNull(), "seco")
        .otherwise("sin dato"),
    )
    .withColumn("updated_ts", F.current_timestamp())
)
upsert_delta(dim_date, f"{catalog}.silver.dim_date", ["date"])

# COMMAND ----------

metrics = {
    "dim_zone_rows": spark.table(f"{catalog}.silver.dim_zone").count(),
    "zones_unknown": spark.table(f"{catalog}.silver.dim_zone").filter("is_unknown").count(),
    "dim_reference_rows": spark.table(f"{catalog}.silver.dim_reference").count(),
    "dim_date_rows": spark.table(f"{catalog}.silver.dim_date").count(),
    "days_without_weather": spark.table(f"{catalog}.silver.dim_date")
    .filter(F.col("weather_condition") == "sin dato").count(),
}
log_metrics(catalog, run_id, "silver", "dimensiones", metrics)
dbutils.notebook.exit(json.dumps(metrics))
