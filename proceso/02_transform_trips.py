# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///


# COMMAND ----------

# MAGIC %md
# MAGIC # 02 · Silver: fact_trips
# MAGIC Por cada mes de `p_months` toma `bronze.yellow_trips` y produce `silver.fact_trips`:
# MAGIC - Identificador determinístico `trip_id` (hash) y eliminación de duplicados exactos.
# MAGIC - Métricas derivadas: duración, velocidad, distancia en km, % de propina, cobro de congestión.
# MAGIC - Enriquecimiento con zonas de origen/destino y descripciones de códigos (joins con `broadcast`).
# MAGIC - **Reglas blandas**: no se descartan, se marcan con banderas `flag_*` e `is_suspicious` para
# MAGIC   que Gold pueda excluirlas de los promedios sin perder trazabilidad.

# COMMAND ----------

# MAGIC %run ./utils/common

# COMMAND ----------

catalog = get_param("p_catalog", "nyc_taxi_dev")
run_id = get_param("p_run_id", "manual")
months = parse_months(get_param("p_months", "2024-12,2025-01,2025-02"))

trips = spark.table(f"{catalog}.bronze.yellow_trips").filter(F.col("source_year_month").isin(months))
zones = spark.table(f"{catalog}.silver.dim_zone")
refs = spark.table(f"{catalog}.silver.dim_reference")


def ref(code_type: str, alias: str) -> DataFrame:
    return F.broadcast(
        refs.filter(F.col("code_type") == code_type)
        .select(F.col("code").alias(f"_{alias}_code"), F.col("description").alias(alias))
    )


pu = F.broadcast(zones.select(
    F.col("location_id").alias("pu_location_id"), F.col("borough").alias("pu_borough"),
    F.col("zone").alias("pu_zone"), F.col("is_airport").alias("pu_is_airport"),
    F.col("is_unknown").alias("pu_is_unknown"),
))
do = F.broadcast(zones.select(
    F.col("location_id").alias("do_location_id"), F.col("borough").alias("do_borough"),
    F.col("zone").alias("do_zone"), F.col("is_airport").alias("do_is_airport"),
    F.col("is_unknown").alias("do_is_unknown"),
))

# COMMAND ----------

key_cols = ["vendor_id", "pickup_datetime", "dropoff_datetime", "pu_location_id", "do_location_id",
            "trip_distance", "fare_amount", "total_amount", "payment_type"]

duration_min = (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60

enriched = (
    trips
    .withColumn("trip_id", F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in key_cols]), 256))
    .withColumn("pickup_date", F.to_date("pickup_datetime"))
    .withColumn("pickup_hour", F.hour("pickup_datetime"))
    .withColumn("pickup_day_of_week", F.dayofweek("pickup_datetime"))
    .withColumn("duration_min", F.round(duration_min, 2))
    .withColumn("trip_distance_km", F.round(F.col("trip_distance") * 1.60934, 3))
    .withColumn("avg_speed_mph",
                F.when(F.col("duration_min") > 0,
                       F.round(F.col("trip_distance") / (F.col("duration_min") / 60), 2)))
    .withColumn("passenger_count",
                F.when(F.col("passenger_count") > 0, F.col("passenger_count")))  # 0 o null -> desconocido
    .withColumn("tip_pct",  # solo tarjeta: las propinas en efectivo no se registran
                F.when((F.col("payment_type") == 1) & (F.col("fare_amount") > 0),
                       F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2)))
    .withColumn("cbd_congestion_fee", F.coalesce("cbd_congestion_fee", F.lit(0.0)))
    .withColumn("airport_fee", F.coalesce("airport_fee", F.lit(0.0)))
    .withColumn("has_cbd_fee", F.col("cbd_congestion_fee") > 0)
    .join(pu, "pu_location_id", "left")
    .join(do, "do_location_id", "left")
    .join(ref("vendor", "vendor_name"), F.col("vendor_id") == F.col("_vendor_name_code"), "left")
    .join(ref("ratecode", "ratecode_desc"), F.col("ratecode_id") == F.col("_ratecode_desc_code"), "left")
    .join(ref("payment_type", "payment_desc"), F.col("payment_type") == F.col("_payment_desc_code"), "left")
    .drop("_vendor_name_code", "_ratecode_desc_code", "_payment_desc_code")
    .fillna({"vendor_name": "Desconocido", "ratecode_desc": "Desconocido", "payment_desc": "Desconocido"})
    .withColumn("is_airport_trip",
                F.coalesce(F.col("pu_is_airport") | F.col("do_is_airport"), F.lit(False))
                | F.col("ratecode_id").isin(2, 3) | (F.col("airport_fee") > 0))
    # ---- reglas blandas ----
    .withColumn("flag_long_duration", F.col("duration_min") > 180)
    .withColumn("flag_zero_distance", (F.col("trip_distance") == 0) & (F.col("fare_amount") > 0))
    .withColumn("flag_high_speed", F.col("avg_speed_mph") > 80)
    .withColumn("flag_extreme_fare", F.col("fare_amount") > 500)
    .withColumn("flag_unknown_zone", F.coalesce(F.col("pu_is_unknown") | F.col("do_is_unknown"), F.lit(True)))
)

flag_cols = ["flag_long_duration", "flag_zero_distance", "flag_high_speed", "flag_extreme_fare", "flag_unknown_zone"]
enriched = enriched.withColumn(
    "is_suspicious", F.greatest(*[F.coalesce(F.col(c), F.lit(False)).cast("int") for c in flag_cols]) == 1
)

fact_trips = enriched.dropDuplicates(["trip_id"]).select(
    "trip_id", "source_year_month", "vendor_id", "vendor_name",
    "pickup_datetime", "dropoff_datetime", "pickup_date", "pickup_hour", "pickup_day_of_week",
    "pu_location_id", "pu_borough", "pu_zone", "do_location_id", "do_borough", "do_zone",
    "passenger_count", "trip_distance", "trip_distance_km", "duration_min", "avg_speed_mph",
    "ratecode_id", "ratecode_desc", "payment_type", "payment_desc",
    "fare_amount", "extra", "mta_tax", "tip_amount", "tip_pct", "tolls_amount", "improvement_surcharge",
    "congestion_surcharge", "airport_fee", "cbd_congestion_fee", "total_amount",
    "has_cbd_fee", "is_airport_trip", *flag_cols, "is_suspicious",
    F.current_timestamp().alias("updated_ts"),
)

replace_partitions(fact_trips, f"{catalog}.silver.fact_trips", "source_year_month", months)

# Compacta archivos y agrupa por zona de origen (filtro más común en Gold y en el dashboard).
# Es mantenimiento, no lógica de negocio: si el cómputo no lo permite, el pipeline continúa
# (en Unity Catalog la optimización predictiva también compacta las tablas administradas).
try:
    DeltaTable.forName(spark, f"{catalog}.silver.fact_trips").optimize().executeZOrderBy("pu_location_id")
    print("OPTIMIZE + ZORDER aplicado a silver.fact_trips")
except Exception as e:
    print(f"[aviso] OPTIMIZE omitido: {str(e)[:300]}")

# COMMAND ----------

written = spark.table(f"{catalog}.silver.fact_trips").filter(F.col("source_year_month").isin(months))

# Todas las métricas en una sola agregación (un solo job en lugar de uno por métrica)
counts = written.agg(
    F.count("*").alias("silver_rows"),
    F.sum(F.col("is_suspicious").cast("int")).alias("suspicious_rows"),
    F.sum(F.col("is_airport_trip").cast("int")).alias("airport_trips"),
    F.sum(F.col("has_cbd_fee").cast("int")).alias("trips_with_cbd_fee"),
    *[F.sum(F.col(c).cast("int")).alias(c) for c in flag_cols],
).first().asDict()

metrics = {"bronze_rows": trips.count(), **{k: int(v or 0) for k, v in counts.items()}}
metrics["duplicates_removed"] = metrics["bronze_rows"] - metrics["silver_rows"]

log_metrics(catalog, run_id, "silver", "fact_trips", metrics)
dbutils.notebook.exit(json.dumps(metrics))
