# Insumos del ETL

| # | Insumo | Formato | Origen | Destino en ADLS (`raw`) |
|---|---|---|---|---|
| 1 | Viajes de taxi amarillo (1 archivo por mes) | Parquet | TLC Trip Record Data | `nyc_taxi/yellow/yellow_tripdata_YYYY-MM.parquet` |
| 2 | Zonas de taxi | CSV | TLC (lookup de zonas) | `nyc_taxi/taxi_zones/taxi_zone_lookup.csv` |
| 3 | Códigos del diccionario de datos | JSON Lines | `datasets/referencias/ref_codes.json` (este repo) | `nyc_taxi/reference/ref_codes.json` |
| 4 | Clima diario | JSON (API REST) | Open-Meteo Historical Weather | `nyc_taxi/weather/` (lo escribe el pipeline) |

## Descarga

Página oficial: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

```
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-12.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-02.parquet
https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Diccionario de datos: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

### Opción A: Azure Data Factory (recomendada)
1. Linked service **HTTP** con URL base `https://d37ci6vzurychx.cloudfront.net/` y autenticación anónima.
2. Linked service **ADLS Gen2** autenticado con la **managed identity** de ADF
   (rol *Storage Blob Data Contributor* sobre el storage).
3. Dataset origen *Binary* sobre HTTP con URL relativa parametrizada `trip-data/yellow_tripdata_@{dataset().year_month}.parquet`.
4. Dataset destino *Binary* en `raw/nyc_taxi/yellow/`.
5. Pipeline con `ForEach` sobre la lista de meses y una *Copy activity* por mes.

### Opción B: manual
Descargar los archivos en el navegador y subirlos con Azure Storage Explorer o `azcopy` a las rutas de la tabla.

## Notas
- Cada mes trae unos 3 a 4 millones de viajes (~60 MB en Parquet). Los archivos no se suben a GitHub.
- Los Parquet cambian de esquema entre periodos (nombres en mayúscula/minúscula, tipos, y
  `cbd_congestion_fee` existe solo desde 2025): el notebook de ingesta lo resuelve.
- La TLC aclara que los datos los reportan los proveedores y no garantiza su exactitud: por eso
  la capa Bronze aplica reglas de calidad y la Silver marca registros sospechosos.
