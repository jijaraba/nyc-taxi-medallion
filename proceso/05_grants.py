# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Grants (mínimo privilegio) con Databricks SDK
# MAGIC | Grupo | Acceso |
# MAGIC |---|---|
# MAGIC | `p_group_engineers` | Lectura de todo el catálogo y de raw. En prod no escribe: solo el pipeline modifica datos |
# MAGIC | `p_group_analysts` | Lectura de Silver y Gold |
# MAGIC | `p_group_bi` | Solo tablas Gold |
# MAGIC
# MAGIC Los permisos de Unity Catalog se heredan (catálogo → esquema → tabla).

# COMMAND ----------

# MAGIC %run ./utils/common

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

catalog = get_param("p_catalog", "nyc_taxi_dev")
eng = get_param("p_group_engineers", "grp_data_engineers")
analysts = get_param("p_group_analysts", "grp_data_analysts")
bi = get_param("p_group_bi", "grp_bi_readers")


def grant(securable_type: str, full_name: str, principal: str, privileges: list) -> None:
    """PATCH a la API de permisos: agrega privilegios sin borrar los existentes (idempotente)."""
    w.api_client.do("PATCH", f"/api/2.1/unity-catalog/permissions/{securable_type}/{full_name}",
                    body={"changes": [{"principal": principal, "add": privileges}]})
    print(f"OK  {securable_type:18} {full_name:35} {principal:20} {privileges}")


plan = [
    ("catalog", catalog, eng, ["USE_CATALOG", "USE_SCHEMA", "SELECT"]),
    ("external_location", "extl_raw", eng, ["READ_FILES"]),
    ("catalog", catalog, analysts, ["USE_CATALOG"]),
    ("schema", f"{catalog}.silver", analysts, ["USE_SCHEMA", "SELECT"]),
    ("schema", f"{catalog}.gold", analysts, ["USE_SCHEMA", "SELECT"]),
    ("catalog", catalog, bi, ["USE_CATALOG"]),
    ("schema", f"{catalog}.gold", bi, ["USE_SCHEMA", "SELECT"]),
]

errors = []
for securable_type, full_name, principal, privileges in plan:
    try:
        grant(securable_type, full_name, principal, privileges)
    except Exception as e:
        errors.append(f"{securable_type} {full_name} -> {principal}: {str(e)[:200]}")

if errors:
    raise Exception("Fallaron grants (¿existen los grupos en la cuenta?):\n" + "\n".join(errors))

# COMMAND ----------

perms = w.api_client.do("GET", f"/api/2.1/unity-catalog/permissions/schema/{catalog}.gold")
for a in perms.get("privilege_assignments", []):
    print(a["principal"], a["privileges"])

dbutils.notebook.exit(f"{len(plan)} grants aplicados")
