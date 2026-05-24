# CDR Silver Pipeline

Solucion en PySpark para construir una pipeline de CDRs a partir de archivos CSV locales. El pipeline lee multiples batches, estandariza el esquema, limpia datos invalidos, deduplica registros y genera datasets listos para consumo analitico.

## Enfoque

La carpeta `data/` simula la capa raw/bronze. El pipeline genera:

- `outputs/silver/cdr_calls`: detalle Silver en Parquet.
- `outputs/silver/customer_aggregates`: agregado por `customer_id` en Parquet.
- `outputs/csv/cdr_calls`: copia CSV del detalle para revision rapida.
- `outputs/csv/customer_aggregates`: copia CSV del agregado.
- `outputs/reports/data_quality_report.json`: informe de calidad.

El esquema esperado es:

```text
call_id, customer_id, call_start, call_end, duration_seconds, call_type,
origin_country, destination_country, cost_usd, ingestion_date
```

Columnas inesperadas, como `record_status`, se ignoran para mantener el esquema Silver consistente y se registran en el informe de calidad.

## Decisiones Tecnicas

- Se usa PySpark para aproximar un procesamiento distribuido tipo Lakehouse.
- La lectura se realiza archivo por archivo para tolerar columnas extra o faltantes.
- Los campos se normalizan y tipan antes de construir Silver.
- `call_type` y paises se normalizan a mayusculas.
- Se descartan registros con `duration_seconds < 0`.
- Los registros con `customer_id` nulo se conservan en el detalle Silver con flag de calidad, pero se excluyen del agregado.
- Se agregan `call_duration_minutes` e `is_international_call`.
- Se genera un agregado por cliente con `total_calls`, `total_duration_minutes` y `total_cost`.
- Se escriben Parquet como salida principal y CSV como salida auxiliar para inspeccion manual.

## Idempotencia

La estrategia de idempotencia usa `record_hash`, un SHA-256 calculado sobre los valores normalizados del registro:

```text
call_id, customer_id, call_start, call_end, duration_seconds, call_type,
origin_country, destination_country, cost_usd, ingestion_date
```

El hash no incluye `source_file`, metadatos tecnicos ni columnas inesperadas. Luego se deduplica por `record_hash`.

En la ejecucion local, los outputs se escriben en modo `overwrite`, por lo que reprocesar los mismos archivos produce el mismo resultado sin acumular duplicados.

## Calidad De Datos

El reporte incluye:

- Conteo de registros por archivo.
- Columnas inesperadas y faltantes.
- Porcentaje de nulos en columnas criticas.
- Conteo de registros descartados por duracion negativa.
- Conteo de duplicados removidos.
- Conteo de registros con `customer_id` nulo.
- Quintiles P20/P40/P60/P80 para `duration_seconds` y `cost_usd`.
- Conteo de candidatos a outlier en colas baja y alta: valores bajo P20 o sobre P80.

## Testing

Se realizaron pruebas unitarias usando `pytest` y una sesion local de Spark. El enfoque tomado es de integracion: se ejecuta el pipeline sobre los CSV reales incluidos en `data/` y se validan los conteos esperados del desafio.

La suite cubre:

- Lectura de multiples CSVs con esquema consistente.
- Deteccion de columnas inesperadas, como `record_status`.
- Normalizacion de tipos y de `call_type`.
- Descarte de duraciones negativas.
- Deduplicacion por `record_hash`.
- Conservacion de `customer_id` nulo en detalle y exclusion del agregado.
- Calculo de columnas derivadas.
- Generacion del reporte de calidad, incluyendo quintiles para `duration_seconds` y `cost_usd`.
- Escritura de salidas Parquet, CSV y JSON.

## Ejecucion

Con ambiente virtual local:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
PYTHONPATH=src python -m cli --input data --output outputs
```

Tests en ambiente virtual:

```bash
PYTHONPATH=src pytest
```

Con Docker:

```bash
docker build -t cdr-silver-pipeline .
docker run --rm -v "$(pwd)/outputs:/app/outputs" cdr-silver-pipeline
```

Ejecutar tests con Docker:

```bash
docker run --rm cdr-silver-pipeline pytest
```

## Adaptacion A OCI Data Flow

La adaptacion a OCI Data Flow seria natural porque la solucion ya esta
desarrollada en PySpark. En un escenario cloud, los archivos CSV de entrada
podrian almacenarse en OCI Object Storage y Data Flow se encargaria de ejecutar
la aplicacion Spark de forma administrada, escribiendo nuevamente los resultados
Silver, agregados y reportes en Object Storage. Para esto, el principal ajuste
seria parametrizar las rutas de entrada y salida para que puedan apuntar tanto a
carpetas locales durante el desarrollo como a ubicaciones en OCI al ejecutar la
pipeline en Data Flow.
