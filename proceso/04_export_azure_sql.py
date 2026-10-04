# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///


# COMMAND ----------

# MAGIC %md
# MAGIC # 04 · (Opcional) Exportar Gold a Azure SQL Database
# MAGIC Publica las tablas Gold en Azure SQL para Power BI u otras aplicaciones. Las credenciales se leen
# MAGIC de un **secret scope respaldado por Azure Key Vault**. Se activa con `p_export_sql = true`.
# MAGIC
# MAGIC **Requisitos en serverless:** el firewall de Azure SQL debe permitir la conexión desde Databricks
# MAGIC (*Networking → Allow Azure services and resources to access this server*).

# COMMAND ----------

# MAGIC %run ./utils/common

# COMMAND ----------

catalog = get_param("p_catalog", "nyc_taxi_dev")
run_id = get_param("p_run_id", "manual")
export_enabled = get_param("p_export_sql", "false").lower() == "true"
secret_scope = get_param("p_secret_scope", "kv-nyctaxi")

if not export_enabled:
    dbutils.notebook.exit("exportación deshabilitada (p_export_sql = false)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Credenciales desde Key Vault

# COMMAND ----------

REQUIRED_SECRETS = ["sql-server", "sql-database", "sql-user", "sql-password"]
try:
    available = {s.key for s in dbutils.secrets.list(secret_scope)}
except Exception as e:
    raise Exception(f"No se pudo leer el secret scope '{secret_scope}'. ¿Existe y tienes permiso?\n{e}")

missing = [k for k in REQUIRED_SECRETS if k not in available]
if missing:
    raise Exception(f"Faltan secretos en '{secret_scope}': {missing}")

server = dbutils.secrets.get(secret_scope, "sql-server")
database = dbutils.secrets.get(secret_scope, "sql-database")
user = dbutils.secrets.get(secret_scope, "sql-user")
password = dbutils.secrets.get(secret_scope, "sql-password")

# Acepta el nombre corto del servidor o el FQDN completo
host = server if server.endswith(".database.windows.net") else f"{server}.database.windows.net"
jdbc_url = (f"jdbc:sqlserver://{host}:1433;database={database};"
            "encrypt=true;trustServerCertificate=false;loginTimeout=30")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exportación

# COMMAND ----------

GOLD_TABLES = ["trips_daily", "hourly_demand", "zone_performance", "top_routes", "payment_tips",
               "congestion_pricing_monthly", "weather_impact"]

metrics = {}
for t in GOLD_TABLES:
    source = f"{catalog}.gold.{t}"
    if not spark.catalog.tableExists(source):
        print(f"[aviso] {source} no existe, se omite (¿se ejecutó 03_load_gold?)")
        metrics[f"{t}_exported"] = 0
        continue

    df = spark.table(source)
    rows = df.count()
    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"dbo.gold_{t}")
        .option("user", user)
        .option("password", password)
        .option("truncate", "true")   # conserva la tabla destino y solo reemplaza las filas
        .mode("overwrite")
        .save()
    )
    metrics[f"{t}_exported"] = rows
    print(f"OK  {source} -> dbo.gold_{t} ({rows} filas)")

log_metrics(catalog, run_id, "gold", "export_azure_sql", metrics)
dbutils.notebook.exit(json.dumps(metrics))
