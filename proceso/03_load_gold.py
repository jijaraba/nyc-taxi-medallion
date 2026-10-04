# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: tablas de negocio
# MAGIC Se recalculan sobre todos los meses cargados. Los promedios usan solo viajes **no sospechosos**;
# MAGIC los conteos totales y los ingresos usan todos los viajes válidos.
# MAGIC
# MAGIC | Tabla | Pregunta de negocio |
# MAGIC |---|---|
# MAGIC | `gold.trips_daily` | ¿Cómo evoluciona la demanda diaria por distrito y cómo influye el clima? |
# MAGIC | `gold.hourly_demand` | ¿En qué días y horas hay más demanda y más congestión (velocidad)? |
# MAGIC | `gold.zone_performance` | ¿Qué zonas generan más viajes e ingresos? |
# MAGIC | `gold.top_routes` | ¿Cuáles son las rutas origen–destino más frecuentes y rentables? |
# MAGIC | `gold.payment_tips` | ¿Cómo pagan los pasajeros y cuánto dejan de propina? |
# MAGIC | `gold.congestion_pricing_monthly` | ¿Qué efecto tuvo el cobro por congestión (desde el 5-ene-2025)? |
# MAGIC | `gold.weather_impact` | ¿La lluvia y la nieve cambian la demanda y las propinas? |

# COMMAND ----------

# MAGIC %run ./utils/common

# COMMAND ----------

catalog = get_param("p_catalog", "nyc_taxi_dev")
run_id = get_param("p_run_id", "manual")
top_routes_per_borough = int(get_param("p_top_routes", "20"))

trips = spark.table(f"{catalog}.silver.fact_trips")
clean = F.col("is_suspicious") == False  # noqa: E712
dim_date = spark.table(f"{catalog}.silver.dim_date")


def avg_clean(col_name: str, alias: str, digits: int = 2) -> Column:
    """Promedio calculado solo sobre viajes no sospechosos."""
    return F.round(F.avg(F.when(clean, F.col(col_name))), digits).alias(alias)


base_aggs = [
    F.count("*").alias("trips"),
    F.round(F.sum("total_amount"), 2).alias("revenue"),
    avg_clean("total_amount", "avg_total_amount"),
    avg_clean("trip_distance_km", "avg_distance_km"),
    avg_clean("duration_min", "avg_duration_min"),
    avg_clean("tip_pct", "avg_tip_pct"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. trips_daily (con clima)

# COMMAND ----------

trips_daily = (
    trips.groupBy("pickup_date", "pu_borough").agg(*base_aggs)
    .join(
        dim_date.select(F.col("date").alias("pickup_date"), "day_name", "is_weekend", "weather_condition",
                        "temperature_max_c", "temperature_min_c", "precipitation_mm", "snowfall_cm"),
        "pickup_date", "left",
    )
)
overwrite_table(trips_daily, f"{catalog}.gold.trips_daily")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. hourly_demand

# COMMAND ----------

DAY_NAMES = {1: "domingo", 2: "lunes", 3: "martes", 4: "miércoles", 5: "jueves", 6: "viernes", 7: "sábado"}
day_map = F.create_map(*[F.lit(x) for kv in DAY_NAMES.items() for x in kv])

hourly = trips.groupBy("pickup_day_of_week", "pickup_hour").agg(
    F.count("*").alias("trips"),
    avg_clean("avg_speed_mph", "avg_speed_mph"),
    avg_clean("total_amount", "avg_total_amount"),
)
total_trips = trips.count()
hourly_demand = (
    hourly.withColumn("day_name", day_map[F.col("pickup_day_of_week")])
    .withColumn("share_pct", F.round(F.col("trips") * 100 / F.lit(total_trips), 3))
    .withColumn("rank_busiest", F.dense_rank().over(Window.orderBy(F.desc("trips"))))
)
overwrite_table(hourly_demand, f"{catalog}.gold.hourly_demand")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. zone_performance

# COMMAND ----------

zone_performance = (
    trips.groupBy("pu_location_id", "pu_borough", "pu_zone")
    .agg(*base_aggs, F.round(F.avg(F.col("is_airport_trip").cast("int")) * 100, 2).alias("airport_trips_pct"))
    .withColumn("share_trips_pct", F.round(F.col("trips") * 100 / F.lit(total_trips), 3))
    .withColumn("rank_in_borough", F.dense_rank().over(Window.partitionBy("pu_borough").orderBy(F.desc("trips"))))
    .withColumn("rank_overall", F.dense_rank().over(Window.orderBy(F.desc("trips"))))
)
overwrite_table(zone_performance, f"{catalog}.gold.zone_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. top_routes (origen → destino)

# COMMAND ----------

routes = (
    trips.filter(clean)
    .groupBy("pu_borough", "pu_zone", "do_borough", "do_zone")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
        F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
        F.round(F.sum("fare_amount") / F.sum("trip_distance"), 2).alias("fare_per_mile"),
    )
    .withColumn("rank_in_borough", F.row_number().over(Window.partitionBy("pu_borough").orderBy(F.desc("trips"))))
    .filter(F.col("rank_in_borough") <= top_routes_per_borough)
)
overwrite_table(routes, f"{catalog}.gold.top_routes")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. payment_tips

# COMMAND ----------

payment_tips = (
    trips.groupBy("pu_borough", "payment_desc")
    .agg(
        F.count("*").alias("trips"),
        F.round(F.sum("tip_amount"), 2).alias("total_tips"),
        avg_clean("tip_pct", "avg_tip_pct"),
    )
    .withColumn("share_in_borough_pct",
                F.round(F.col("trips") * 100 / F.sum("trips").over(Window.partitionBy("pu_borough")), 2))
)
overwrite_table(payment_tips, f"{catalog}.gold.payment_tips")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. congestion_pricing_monthly
# MAGIC Compara meses antes y después del cobro por la Zona de Alivio de Congestión (5-ene-2025).
# MAGIC La velocidad promedio en Manhattan sirve como indicador de tráfico.

# COMMAND ----------

manhattan = F.col("pu_borough") == "Manhattan"
congestion = trips.groupBy("source_year_month").agg(
    F.count("*").alias("trips"),
    F.sum(F.col("has_cbd_fee").cast("int")).alias("trips_with_cbd_fee"),
    F.round(F.sum("cbd_congestion_fee"), 2).alias("cbd_fee_revenue"),
    F.round(F.sum("total_amount"), 2).alias("revenue"),
    avg_clean("total_amount", "avg_total_amount"),
    F.round(F.avg(F.when(clean & manhattan, F.col("avg_speed_mph"))), 2).alias("avg_speed_manhattan_mph"),
    F.round(F.avg(F.when(clean & manhattan, F.col("duration_min"))), 2).alias("avg_duration_manhattan_min"),
)
w_month = Window.orderBy("source_year_month")
congestion_pricing_monthly = (
    congestion
    .withColumn("cbd_fee_trips_pct", F.round(F.col("trips_with_cbd_fee") * 100 / F.col("trips"), 2))
    .withColumn("trips_change_pct",
                F.round((F.col("trips") - F.lag("trips").over(w_month)) * 100 / F.lag("trips").over(w_month), 2))
    .withColumn("period", F.when(F.col("source_year_month") >= "2025-01", "con cobro").otherwise("sin cobro"))
)
overwrite_table(congestion_pricing_monthly, f"{catalog}.gold.congestion_pricing_monthly")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. weather_impact

# COMMAND ----------

daily_city = (
    trips.groupBy("pickup_date").agg(F.count("*").alias("trips"), avg_clean("tip_pct", "avg_tip_pct"))
    .join(dim_date.select(F.col("date").alias("pickup_date"), "weather_condition", "is_weekend"), "pickup_date")
)
weather_impact = daily_city.groupBy("weather_condition", "is_weekend").agg(
    F.count("*").alias("days"),
    F.round(F.avg("trips"), 0).alias("avg_daily_trips"),
    F.round(F.avg("avg_tip_pct"), 2).alias("avg_tip_pct"),
)
overwrite_table(weather_impact, f"{catalog}.gold.weather_impact")

# COMMAND ----------

GOLD_TABLES = ["trips_daily", "hourly_demand", "zone_performance", "top_routes", "payment_tips",
               "congestion_pricing_monthly", "weather_impact"]
metrics = {f"{t}_rows": spark.table(f"{catalog}.gold.{t}").count() for t in GOLD_TABLES}
log_metrics(catalog, run_id, "gold", "negocio", metrics)
dbutils.notebook.exit(json.dumps(metrics))
