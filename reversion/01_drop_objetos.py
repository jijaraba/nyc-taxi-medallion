# Databricks notebook source
# MAGIC %md
# MAGIC # Reversión lógica · eliminar objetos de Unity Catalog (Python, Databricks SDK)
# MAGIC Elimina tablas, esquemas y catálogo del ambiente indicado. Después ejecutar
# MAGIC `02_borrar_rutas_fisicas.py` para limpiar los archivos en ADLS.
# MAGIC - `p_confirm = SI_BORRAR` es obligatorio.
# MAGIC - `p_drop_infra = true` elimina además external locations y storage credential
# MAGIC   (solo al desmontar todo el proyecto).

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

dbutils.widgets.text("p_catalog", "nyc_taxi_dev")
dbutils.widgets.text("p_confirm", "")
dbutils.widgets.dropdown("p_drop_infra", "false", ["false", "true"])

catalog = dbutils.widgets.get("p_catalog").strip()
confirm = dbutils.widgets.get("p_confirm").strip()
drop_infra = dbutils.widgets.get("p_drop_infra") == "true"

if confirm != "SI_BORRAR":
    dbutils.notebook.exit("Cancelado: escribe SI_BORRAR en p_confirm para continuar")


def safe(label, fn, **kwargs):
    try:
        fn(**kwargs)
        print("Eliminado:", label)
    except Exception as e:
        if "does not exist" in str(e).lower() or "NOT_FOUND" in str(e) or "NotFound" in type(e).__name__:
            print("No existe:", label)
        else:
            raise

# COMMAND ----------

TABLES = {
    "gold": ["trips_daily", "hourly_demand", "zone_performance", "top_routes", "payment_tips",
             "congestion_pricing_monthly", "weather_impact"],
    "silver": ["fact_trips", "dim_zone", "dim_reference", "dim_date"],
    "bronze": ["yellow_trips", "taxi_zones", "ref_codes", "weather_daily", "quarantine"],
    "audit": ["data_quality_log"],
}

for schema, names in TABLES.items():
    for t in names:
        safe(f"tabla {catalog}.{schema}.{t}", w.tables.delete, full_name=f"{catalog}.{schema}.{t}")
for schema in TABLES:
    safe(f"esquema {catalog}.{schema}", w.schemas.delete, full_name=f"{catalog}.{schema}", force=True)
safe(f"catálogo {catalog}", w.catalogs.delete, name=catalog, force=True)

# COMMAND ----------

if drop_infra:
    for container in ["raw", "bronze", "silver", "gold", "catalog"]:
        safe(f"external location extl_{container}", w.external_locations.delete,
             name=f"extl_{container}", force=True)
    safe("storage credential cred_nyctaxi_mi", w.storage_credentials.delete, name="cred_nyctaxi_mi", force=True)

dbutils.notebook.exit("reversión lógica completada")
