# Databricks notebook source
# MAGIC %md
# MAGIC # utils/common
# MAGIC Funciones compartidas por todos los notebooks del pipeline NYC Taxi.
# MAGIC Se importan con `%run ./utils/common`.
# MAGIC
# MAGIC | Función | Propósito |
# MAGIC |---|---|
# MAGIC | `get_param` / `parse_months` | Widgets y validación del parámetro de meses (`YYYY-MM`) |
# MAGIC | `raw_path` | Ruta `abfss://` de la capa raw (external location + Managed Identity) |
# MAGIC | `read_raw` | Lectura CSV/JSON con esquema explícito y captura de registros corruptos |
# MAGIC | `read_parquet_with_drift` | Lectura Parquet tolerante a cambios de esquema entre meses |
# MAGIC | `split_valid_invalid` | Separa registros válidos e inválidos según reglas de calidad |
# MAGIC | `send_to_quarantine` | Guarda los inválidos en `bronze.quarantine` (idempotente por entidad) |
# MAGIC | `add_audit_columns` | Columnas de auditoría |
# MAGIC | `upsert_delta` | MERGE idempotente (dimensiones) |
# MAGIC | `replace_partitions` | Reemplazo de particiones con `replaceWhere` (hechos por mes) |
# MAGIC | `overwrite_table` | Reescritura completa (Gold) |
# MAGIC | `log_metrics` | Métricas en `audit.data_quality_log` |

# COMMAND ----------

import json
import re

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------


def get_param(name: str, default: str = "") -> str:
    """Crea el widget si no existe y devuelve su valor (los parámetros del job lo sobrescriben)."""
    dbutils.widgets.text(name, default)
    return dbutils.widgets.get(name).strip()


def parse_months(value: str) -> list:
    """'2024-12, 2025-01' -> ['2024-12', '2025-01'] validando el formato YYYY-MM."""
    months = sorted({m.strip() for m in value.split(",") if m.strip()})
    bad = [m for m in months if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", m)]
    if not months or bad:
        raise ValueError(f"p_months inválido: {value!r}. Formato esperado: YYYY-MM,YYYY-MM")
    return months


def raw_path(storage_account: str, entity: str) -> str:
    """Ruta de la capa raw en ADLS Gen2. Nunca DBFS ni Volumes."""
    return f"abfss://raw@{storage_account}.dfs.core.windows.net/nyc_taxi/{entity}/"


def read_raw(path: str, schema: StructType, file_format: str = "csv", delimiter: str = ",") -> DataFrame:
    """Lee CSV o JSON (una línea por registro) con esquema explícito en modo PERMISSIVE.
    Las filas mal formadas quedan en `_corrupt_record`. Agrega `source_file`."""
    schema_with_corrupt = StructType(schema.fields + [StructField("_corrupt_record", StringType(), True)])
    reader = (
        spark.read.format(file_format)
        .schema(schema_with_corrupt)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
    )
    if file_format == "csv":
        reader = (reader.option("header", "true").option("sep", delimiter)
                  .option("quote", '"').option("escape", '"'))
    return reader.load(path).withColumn("source_file", F.col("_metadata.file_path"))


def read_parquet_with_drift(path: str, target_schema: StructType, rename_map: dict) -> DataFrame:
    """Lee un archivo Parquet y lo lleva al esquema destino.

    Los archivos de la TLC cambian entre meses (p. ej. `Airport_fee` vs `airport_fee`,
    `passenger_count` double vs bigint, `cbd_congestion_fee` solo desde 2025). Se normalizan
    los nombres a minúsculas, se renombran, se castean y las columnas ausentes llegan como null.
    """
    df = spark.read.parquet(path).withColumn("source_file", F.col("_metadata.file_path"))
    available = {c.lower(): c for c in df.columns}
    cols = []
    for source_lower, target in rename_map.items():
        dtype = target_schema[target].dataType
        if source_lower in available:
            cols.append(F.col(f"`{available[source_lower]}`").cast(dtype).alias(target))
        else:
            cols.append(F.lit(None).cast(dtype).alias(target))
    missing = [t for s, t in rename_map.items() if s not in available]
    if missing:
        print(f"[schema drift] {path}: columnas ausentes -> {missing}")
    return df.select(*cols, "source_file").withColumn("_corrupt_record", F.lit(None).cast("string"))


def split_valid_invalid(df: DataFrame, required_cols: list, extra_rule: Column = None):
    """Devuelve (validos, invalidos). Válido = no corrupto, sin nulos obligatorios y cumple la regla."""
    rule = F.col("_corrupt_record").isNull()
    for c in required_cols:
        rule = rule & F.col(c).isNotNull()
    if extra_rule is not None:
        rule = rule & extra_rule
    flagged = df.withColumn("_is_valid", F.coalesce(rule, F.lit(False)))
    valid = flagged.filter(F.col("_is_valid")).drop("_is_valid", "_corrupt_record")
    invalid = flagged.filter(~F.col("_is_valid")).drop("_is_valid")
    return valid, invalid


def send_to_quarantine(invalid: DataFrame, catalog: str, entity: str, run_id: str) -> int:
    """Reemplaza la cuarentena de esa entidad (re-ejecutar no duplica) y devuelve cuántos registros fueron."""
    data_cols = [c for c in invalid.columns if c not in ("_corrupt_record", "source_file")]
    out = invalid.select(
        F.lit(entity).alias("entity"),
        F.coalesce(F.col("_corrupt_record"), F.to_json(F.struct(*data_cols))).alias("raw_record"),
        F.col("source_file"),
        F.lit(run_id).alias("run_id"),
        F.current_timestamp().alias("quarantine_ts"),
    )
    (out.write.format("delta").mode("overwrite")
        .option("replaceWhere", f"entity = '{entity}'")
        .saveAsTable(f"{catalog}.bronze.quarantine"))
    return spark.table(f"{catalog}.bronze.quarantine").filter(F.col("entity") == entity).count()


def add_audit_columns(df: DataFrame, data_source: str, run_id: str) -> DataFrame:
    return df.withColumns({
        "ingestion_ts": F.current_timestamp(),
        "data_source": F.lit(data_source),
        "run_id": F.lit(run_id),
    })


def upsert_delta(df: DataFrame, table: str, keys: list) -> None:
    """MERGE idempotente por llave (dimensiones y catálogos)."""
    df = df.dropDuplicates(keys)
    if not spark.catalog.tableExists(table):
        df.write.format("delta").saveAsTable(table)
        return
    condition = " AND ".join([f"t.{k} = s.{k}" for k in keys])
    (DeltaTable.forName(spark, table).alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())


def replace_partitions(df: DataFrame, table: str, partition_col: str, values: list) -> None:
    """Sobrescribe solo las particiones indicadas (carga incremental idempotente por mes).
    Mucho más eficiente que un MERGE de millones de filas sin llave natural."""
    condition = f"{partition_col} IN ({', '.join(repr(v) for v in values)})"
    if not spark.catalog.tableExists(table):
        df.write.format("delta").partitionBy(partition_col).saveAsTable(table)
        return
    (df.write.format("delta").mode("overwrite")
        .option("replaceWhere", condition)
        .saveAsTable(table))


def overwrite_table(df: DataFrame, table: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def log_metrics(catalog: str, run_id: str, layer: str, entity: str, metrics: dict) -> None:
    rows = [(run_id, layer, entity, k, int(v)) for k, v in metrics.items()]
    schema = "run_id STRING, layer STRING, entity STRING, metric STRING, value BIGINT"
    (spark.createDataFrame(rows, schema)
        .withColumn("log_ts", F.current_timestamp())
        .write.mode("append")
        .saveAsTable(f"{catalog}.audit.data_quality_log"))
    print(f"[{layer}.{entity}] {metrics}")
