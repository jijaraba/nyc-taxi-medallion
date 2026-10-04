# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # PrepAmb · Preparación del ambiente (admin, una sola vez)
# MAGIC Crea con **Python** (Databricks SDK, sin SQL):
# MAGIC 1. Storage credential con **Managed Identity** (Access Connector).
# MAGIC 2. External locations por contenedor.
# MAGIC 3. Catálogos `nyc_taxi_dev` y `nyc_taxi_prod` con sus esquemas por capa.
# MAGIC 4. Permisos del service principal del pipeline.
# MAGIC
# MAGIC Las tablas las crea `proceso/00_prep_ambiente.py` en cada ejecución del workflow.
# MAGIC
# MAGIC **Antes (en Azure):** Access Connector con rol *Storage Blob Data Contributor* sobre el
# MAGIC storage y contenedores `raw`, `bronze`, `silver`, `gold`, `catalog`.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

dbutils.widgets.text("p_storage_account", "<storage_account>")
dbutils.widgets.text("p_access_connector_id",
    "/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Databricks/accessConnectors/<nombre>")
dbutils.widgets.text("p_credential_name", "cred_nyctaxi_mi")
dbutils.widgets.text("p_pipeline_sp", "<application-id-del-service-principal>")

storage_account = dbutils.widgets.get("p_storage_account").strip()
access_connector_id = dbutils.widgets.get("p_access_connector_id").strip()
credential_name = dbutils.widgets.get("p_credential_name").strip()
pipeline_sp = dbutils.widgets.get("p_pipeline_sp").strip()

CONTAINERS = ["raw", "bronze", "silver", "gold", "catalog"]
CATALOGS = ["nyc_taxi_dev", "nyc_taxi_prod"]
LAYERS = ["bronze", "silver", "gold"]


def abfss(container: str, path: str = "") -> str:
    return f"abfss://{container}@{storage_account}.dfs.core.windows.net/{path}"


def create_if_missing(label: str, fn, **kwargs):
    """Considera 'ya existe' como éxito, así el notebook es idempotente."""
    try:
        fn(**kwargs)
        print("Creado:   ", label)
    except Exception as e:
        if "already exists" in str(e).lower() or "ALREADY_EXISTS" in str(e):
            print("Existente:", label)
        else:
            raise


def grant(securable_type: str, full_name: str, principal: str, privileges: list):
    w.api_client.do("PATCH", f"/api/2.1/unity-catalog/permissions/{securable_type}/{full_name}",
                    body={"changes": [{"principal": principal, "add": privileges}]})
    print(f"Grant {privileges} en {securable_type} {full_name} -> {principal}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Storage credential con Managed Identity

# COMMAND ----------

create_if_missing(
    f"storage credential {credential_name}",
    w.api_client.do,
    method="POST",
    path="/api/2.1/unity-catalog/storage-credentials",
    body={
        "name": credential_name,
        "azure_managed_identity": {"access_connector_id": access_connector_id},
        "comment": "Managed Identity del Access Connector - proyecto NYC Taxi",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. External locations

# COMMAND ----------

for container in CONTAINERS:
    create_if_missing(
        f"external location extl_{container}",
        w.external_locations.create,
        name=f"extl_{container}",
        url=abfss(container),
        credential_name=credential_name,
        comment=f"Contenedor {container} del proyecto NYC Taxi",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Catálogos y esquemas

# COMMAND ----------

for catalog in CATALOGS:
    create_if_missing(f"catálogo {catalog}", w.catalogs.create,
                      name=catalog, storage_root=abfss("catalog", catalog),
                      comment="NYC Taxi - arquitectura medallion")
    for layer in LAYERS:
        create_if_missing(f"esquema {catalog}.{layer}", w.schemas.create,
                          name=layer, catalog_name=catalog, storage_root=abfss(layer, catalog))
    create_if_missing(f"esquema {catalog}.audit", w.schemas.create, name="audit", catalog_name=catalog)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Permisos del service principal del pipeline
# MAGIC Lee y escribe en raw (el clima se descarga de la API a raw) y es dueño operativo de prod.

# COMMAND ----------

if not pipeline_sp.startswith("<"):
    grant("external_location", "extl_raw", pipeline_sp, ["READ_FILES", "WRITE_FILES"])
    for container in ["bronze", "silver", "gold", "catalog"]:
        grant("external_location", f"extl_{container}", pipeline_sp, ["CREATE_MANAGED_STORAGE", "READ_FILES", "WRITE_FILES"])
    # ALL_PRIVILEGES no incluye MANAGE: sin MANAGE el SP no puede otorgar permisos (tarea grants)
    grant("catalog", "nyc_taxi_prod", pipeline_sp, ["ALL_PRIVILEGES", "MANAGE"])

# COMMAND ----------

for loc in w.external_locations.list():
    if loc.name.startswith("extl_"):
        print(f"{loc.name:15} {loc.url}  (credencial: {loc.credential_name})")
