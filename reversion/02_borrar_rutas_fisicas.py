# Databricks notebook source
# MAGIC %md
# MAGIC # Reversión física · borrar archivos en ADLS
# MAGIC Unity Catalog purga con retraso los archivos de tablas administradas. Este notebook limpia de
# MAGIC inmediato las rutas del ambiente. **No toca la capa raw**, salvo que `p_include_weather = true`
# MAGIC (el JSON del clima lo genera el pipeline y se puede volver a descargar).

# COMMAND ----------

dbutils.widgets.text("p_catalog", "nyc_taxi_dev")
dbutils.widgets.text("p_storage_account", "<storage_account>")
dbutils.widgets.text("p_confirm", "")
dbutils.widgets.dropdown("p_include_weather", "false", ["false", "true"])

catalog = dbutils.widgets.get("p_catalog").strip()
storage_account = dbutils.widgets.get("p_storage_account").strip()
confirm = dbutils.widgets.get("p_confirm").strip()
include_weather = dbutils.widgets.get("p_include_weather") == "true"

if confirm != "SI_BORRAR":
    dbutils.notebook.exit("Cancelado: escribe SI_BORRAR en p_confirm para continuar")

# COMMAND ----------

paths = [f"abfss://{c}@{storage_account}.dfs.core.windows.net/{catalog}"
         for c in ["bronze", "silver", "gold", "catalog"]]
if include_weather:
    paths.append(f"abfss://raw@{storage_account}.dfs.core.windows.net/nyc_taxi/weather")

for path in paths:
    try:
        dbutils.fs.rm(path, recurse=True)
        print("Borrado:", path)
    except Exception as e:
        print("No existe o sin permiso:", path, "->", str(e)[:120])

dbutils.notebook.exit("reversión física completada")
