# Market Basket para Layout de Picking

Proyecto Python orientado a operaciones logisticas para detectar afinidad entre articulos de almacen y convertirla en recomendaciones utiles para layout de picking.

La logica de negocio central se mantiene:

`transaction_id = Pedido externo + Propietario`

Esto evita mezclar preparaciones fisicas distintas cuando un mismo pedido externo aparece con varios propietarios.

## Objetivo operativo

El foco no es retail generico. El objetivo es identificar articulos que se preparan juntos para:

- acercarlos fisicamente en el almacen
- reducir recorrido de picking
- aumentar productividad
- preparar una futura fase de optimizacion espacial con layout actual vs layout recomendado

## Arquitectura

```text
.
|-- config/
|   `-- default_config.yaml
|-- src/
|   `-- market_basket/
|       |-- __init__.py
|       |-- associations.py
|       |-- cleaning.py
|       |-- clustering.py
|       |-- config.py
|       |-- eda.py
|       |-- io.py
|       |-- outputs.py
|       |-- pipeline.py
|       |-- scoring.py
|       |-- similarity.py
|       |-- temporal.py
|       |-- transactions.py
|       `-- utils.py
|-- tests/
|   |-- test_associations.py
|   |-- test_cleaning.py
|   |-- test_config.py
|   |-- test_temporal_scoring.py
|   |-- test_transactions.py
|   `-- test_utils.py
|-- main.py
`-- requirements.txt
```

## Ejecucion

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python main.py --config config/default_config.yaml
```

Tras ejecutar:

```bash
python main.py --config config/default_config.yaml
```

el pipeline vuelve a generar automaticamente las salidas en:

- `output/`
- `output/logs/`
- `output/plots/`

## Tests

La suite minima incluida usa `unittest`, asi que no requiere dependencias extra:

```bash
python -m unittest discover -s tests -v
```

## Decisiones de calidad de datos

El pipeline incorpora reglas configurables en `data_quality`:

- exclusion opcional de filas con `quantity <= 0`
- eliminacion opcional de duplicados exactos
- definicion configurable del subset de columnas para detectar duplicados
- log de exclusiones aplicadas y trazabilidad en metadata

Reglas activas por defecto:

- filtrar `Tipo movimiento = PI`
- excluir filas sin `Pedido externo` del modelo principal
- excluir filas con `quantity <= 0`
- no eliminar duplicados exactos salvo que se active en YAML

Outputs especificos de calidad:

- `calidad_datos`
- `output/logs/resumen_nulos.csv`
- `output/logs/filas_excluidas_sin_pedido_externo.csv`
- `sku_location_profile`

## Ingesta y configuracion

El bloque `paths` permite:

- `input_data` con Excel, CSV o Parquet
- `sheet_name` configurable para Excel
- rutas separadas para outputs, logs y plots

Los bloques nuevos de configuracion son:

- `data_quality`
- `transaction`
- `thresholds`
- `performance`

La validacion de configuracion comprueba:

- existencia de ruta de entrada
- rangos de thresholds
- bins del score
- estrategia de fecha de transaccion
- coherencia de `score_weights`

Si los pesos del score no suman 1 y `model.score_weight_policy = normalize`, se normalizan automaticamente.

## Modelo

### 1. Cleaning

`cleaning.py`:

- parsea fechas y cantidades
- registra nulos e incidencias
- aplica reglas de exclusion configurables
- genera `sku_location_profile`
- enriquece `sku_attributes` con:
  - `dominant_location_share`
  - `latest_location`
  - `multi_location_flag`

### 2. Transacciones

`transactions.py` mantiene:

- `transaction_id = external_order + owner`

Y añade:

- separador configurable
- estrategia configurable de fecha:
  - `max_completion_date`
  - `min_completion_date`
  - `mode_date`
- nuevas metricas:
  - `unique_locations_in_basket`
  - `repeated_sku_flag`
  - `basket_dispersion_proxy`

### 3. Asociaciones

`associations.py` calcula:

- soporte
- confianza A->B y B->A
- lift
- leverage
- conviction
- Jaccard
- cosine
- weighted cosine
- PMI
- NPMI
- residual cooccurrence

Los thresholds se separan por tipo:

- `thresholds.pairs`
- `thresholds.rules`
- `thresholds.clustering`
- `thresholds.scoring`

Tambien queda preparado el backend para una futura implementacion con matrices dispersas si el volumen crece.

### 4. Temporal

`temporal.py` separa:

1. calculo bruto por periodo con filtro minimo muy bajo
2. calculo de estabilidad sobre la serie temporal bruta

Metricas nuevas:

- `support_slope`
- `lift_slope`
- `support_trend`
- `lift_trend`
- `trend_classification`

Esto evita que la estabilidad dependa solo de si una relacion supero thresholds estrictos en cada periodo.

### 5. Score de layout

`scoring.py` mantiene el score en rango `[0, 1]` y lo hace mas interpretable:

```text
score = weighted_score_pre_penalty
      * operational_relevance_factor
      * popularity_penalty
```

Componentes:

- frecuencia conjunta normalizada
- lift suavizado
- confianza bidireccional equilibrada
- similitud media
- estabilidad temporal
- volumen compartido

Columnas explicativas nuevas:

- `layout_action_hint`
- `candidate_same_slot_area`
- `candidate_same_zone`
- `candidate_manual_review`

Interpretacion practica:

- `very_high`: candidato fuerte a cercania fisica directa
- `high`: buen candidato a misma zona o frente cercano
- `medium`: revisar con contexto operativo adicional
- `low`: monitorizar, no priorizar layout salvo evidencia externa

### 6. Clustering

`clustering.py` mantiene el enfoque de grafo y comunidades, pero mejora:

- filtrado previo de edges
- ordenacion explicita antes del recorte
- metricas de cluster:
  - `density`
  - `mean_intra_cluster_score`
  - `mean_intra_cluster_lift`

Tambien queda preparado para soportar metodos alternativos en el futuro.

## Outputs

Salidas principales en `output/`:

- `kpi_resumen`
- `calidad_datos`
- `transacciones_resumen`
- `articulos_resumen`
- `articulos_por_propietario`
- `sku_location_profile`
- `item_metrics`
- `afinidad_pares`
- `afinidad_reglas`
- `clusters_sku`
- `hubs_sku`
- `raw_temporal_pairs`
- `temporal_stability_metrics`
- `series_temporales`
- `metadata_modelo.json`
- `resumen_ejecutivo.md`

Visualizaciones en `output/plots/`:

- histograma de tamano de cesta
- top articulos por frecuencia
- heatmap de afinidad para top articulos
- red de afinidad entre SKUs
- evolucion temporal de relaciones clave
- distribucion del score final

## Notas sobre outputs generados y GitHub

El pipeline genera automaticamente resultados en `output/`, `output/logs/` y `output/plots/`.
Esos ficheros son artefactos generados de ejecucion y no forman parte del codigo fuente.
Algunos pueden crecer mucho, por ejemplo `raw_temporal_pairs.csv` o `series_temporales.csv`, y superar el limite de 100 MB de GitHub.

Por ese motivo `output/` debe estar en `.gitignore`.
Ignorar `output/` en Git no impide que el pipeline vuelva a crear esos archivos: simplemente evita que se versionen por error.

### Caso A: `output/` ya esta trackeado y quieres quitarlo del indice sin borrar los archivos locales

```bash
git rm -r --cached output
git add .gitignore
git commit -m "Ignore generated outputs"
git push
```

### Caso B: GitHub sigue rechazando el push porque esos ficheros grandes ya entraron en el ultimo commit local

```bash
git reset HEAD~1
git rm -r --cached output
git add .gitignore
git add .
git commit -m "Initial commit without generated outputs"
git push -f origin main
```

## Resumen ejecutivo

`output/resumen_ejecutivo.md` incluye:

- oportunidades prioritarias de cercania
- relaciones fuertes pero con baja recurrencia
- relaciones emergentes
- clusters operativos destacados
- SKUs hub operativos
- riesgos y sesgos de datos

## Limitaciones conocidas

- la afinidad no implica por si sola una recolocacion optima; todavia no se modela distancia fisica real
- `Ubicacion` en movimientos puede reflejar varias ubicaciones observadas para un SKU, no necesariamente un maestro unico
- los pares muy raros con lift extremo siguen siendo utiles para exploracion, pero no deben ejecutar cambios de layout sin validacion operativa
- el backend actual de pares usa combinaciones por transaccion; esta preparado para evolucionar a enfoque disperso si el volumen crece mas

## Preparacion para la futura union con foto de stock o layout

El proyecto ya deja las claves necesarias para la siguiente fase:

- union por `article`
- union por `location`
- lectura de `sku_location_profile`
- uso de `latest_location` y `primary_location`

La siguiente fase recomendada es cruzar:

- `afinidad_pares`
- `clusters_sku`
- `hubs_sku`
- maestro de ubicaciones o foto de stock

Con eso podras comparar:

- layout actual
- layout recomendado
- separacion fisica entre articulos afinados
- ahorro potencial de recorrido

## Dataset real ya analizado

Sobre el dataset actual del repositorio:

- 291.237 filas totales
- 66.336 movimientos `PI`
- 63.378 filas validas para el modelo principal
- 21.120 transacciones validas
- 7.847 SKUs unicos
- 74 propietarios
- 60,59% de transacciones con un solo articulo

Ademas:

- hay una proporcion relevante de filas sin `Pedido externo`
- hay muchas observaciones sin `Ubicacion`
- existen SKUs con multiples ubicaciones observadas

Todo ello queda trazado en outputs y metadata para que las decisiones de layout no pierdan contexto de calidad de dato.
