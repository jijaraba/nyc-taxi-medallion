# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///


# COMMAND ----------

# MAGIC %md
# MAGIC # 04 · (Opcional) Exportar Gold a Azure SQL Database
# MAGIC Publica las tablas Gold en Azure SQL para Power BI u otras aplicaciones. Credenciales desde un
# MAGIC **secret scope respaldado por Azure Key Vault**. Se activa con `p_export_sql = true`.

# COMMAND ----------

# MAGIC %run ./utils/common

# COMMAND ----------

catalog = get_param("p_catalog", "nyc_taxi_dev")
run_id = get_param("p_run_id", "manual")
export_enabled = get_param("p_export_sql", "false").lower() == "true"
secret_scope = get_param("p_secret_scope", "kv-nyctaxi")

if not export_enabled:
    dbutils.notebook.exit("exportación deshabilitada (p_export_sql = false)")

server = dbutils.secrets.get(secret_scope, "sql-server")
database = dbutils.secrets.get(secret_scope, "sql-database")
jdbc_url = f"jdbc:sqlserver://{server}.database.windows.net:1433;database={database};encrypt=true;loginTimeout=30"
user = dbutils.secrets.get(secret_scope, "sql-user")
password = dbutils.secrets.get(secret_scope, "sql-password")

# COMMAND ----------

GOLD_TABLES = ["trips_daily", "hourly_demand", "zone_performance", "top_routes", "payment_tips",
               "congestion_pricing_monthly", "weather_impact"]
metrics = {}
for t in GOLD_TABLES:
    df = spark.table(f"{catalog}.gold.{t}")
    (df.write.format("jdbc")
       .option("url", jdbc_url).option("dbtable", f"dbo.gold_{t}")
       .option("user", user).option("password", password)
       .option("truncate", "true").mode("overwrite").save())
    metrics[f"{t}_exported"] = df.count()

log_metrics(catalog, run_id, "gold", "export_azure_sql", metrics)
dbutils.notebook.exit(json.dumps(metrics))
