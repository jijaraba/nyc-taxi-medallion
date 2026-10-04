# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Seguridad · grupos y grants (Python, Databricks SDK)
# MAGIC 1. Crear los grupos a nivel de **cuenta** (Account console → User management → Groups):
# MAGIC    `grp_data_engineers`, `grp_data_analysts`, `grp_bi_readers`, y asignarlos al workspace.
# MAGIC 2. Ejecutar este notebook. Aplica el modelo en **ambos ambientes**:
# MAGIC    - **dev:** ingenieros con control total.
# MAGIC    - **prod:** mínimo privilegio (lo mismo que `proceso/05_grants.py` en el workflow).
# MAGIC
# MAGIC El dataset no contiene datos personales (no hay nombres, placas ni coordenadas exactas),
# MAGIC por eso no se requiere enmascaramiento.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

ENG, ANALYSTS, BI = "grp_data_engineers", "grp_data_analysts", "grp_bi_readers"
DEV, PROD = "nyc_taxi_dev", "nyc_taxi_prod"


def change(securable_type, full_name, principal, privileges, action="add"):
    w.api_client.do("PATCH", f"/api/2.1/unity-catalog/permissions/{securable_type}/{full_name}",
                    body={"changes": [{"principal": principal, action: privileges}]})
    print(f"{action.upper():6} {securable_type:18} {full_name:30} {principal:20} {privileges}")

# COMMAND ----------

# Desarrollo
change("catalog", DEV, ENG, ["ALL_PRIVILEGES"])
change("external_location", "extl_raw", ENG, ["READ_FILES"])

# Producción
change("catalog", PROD, ENG, ["USE_CATALOG", "USE_SCHEMA", "SELECT"])
change("catalog", PROD, ENG, ["MODIFY", "CREATE_TABLE"], action="remove")  # solo el pipeline escribe
change("catalog", PROD, ANALYSTS, ["USE_CATALOG"])
change("schema", f"{PROD}.silver", ANALYSTS, ["USE_SCHEMA", "SELECT"])
change("schema", f"{PROD}.gold", ANALYSTS, ["USE_SCHEMA", "SELECT"])
change("catalog", PROD, BI, ["USE_CATALOG"])
change("schema", f"{PROD}.gold", BI, ["USE_SCHEMA", "SELECT"])

# COMMAND ----------

for securable_type, name in [("catalog", PROD), ("schema", f"{PROD}.silver"), ("schema", f"{PROD}.gold")]:
    perms = w.api_client.do("GET", f"/api/2.1/unity-catalog/permissions/{securable_type}/{name}")
    print(f"\n{securable_type} {name}")
    for a in perms.get("privilege_assignments", []):
        print("  ", a["principal"], a["privileges"])