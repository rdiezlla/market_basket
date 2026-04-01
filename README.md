# Market Basket para Layout de Picking

Proyecto Python para analizar movimientos de almacen y detectar afinidades reales entre SKUs con foco logistico: reducir recorrido de picking, agrupar articulos que salen juntos y preparar una futura fase de optimizacion espacial del layout.

La regla de negocio central del proyecto es:

`transaction_id = Pedido externo + Propietario`

No se agrupa solo por `Pedido externo`, porque un mismo pedido puede aparecer con varios propietarios y corresponder a preparaciones fisicas distintas.

## Vista Rapida

```text
Input principal
movimientos.xlsx / csv / parquet
        |
        v
1. Ingesta robusta
2. Limpieza y calidad
3. Construccion de transacciones
4. Afinidad SKU-SKU
5. Estabilidad temporal
6. Score de layout
7. Clusters y hubs
8. Outputs tabulares + plots + resumen ejecutivo
        |
        v
output/
output/logs/
output/plots/
```

## Para Que Sirve

El proyecto ayuda a responder preguntas operativas como:

- Que articulos conviene acercar fisicamente en el almacen
- Que pares son frecuentes y estables, y cuales son relaciones raras pero interesantes
- Que familias operativas emergen de la coocurrencia real
- Que SKUs actuan como hubs y conectan muchas preparaciones
- Que informacion de calidad de dato puede sesgar decisiones de layout

## Como Ejecutarlo

### Requisitos

- Python 3.12 recomendado
- Fichero de entrada con movimientos en Excel, CSV o Parquet
- Configuracion en `config/default_config.yaml`

### Windows

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python main.py --config config/default_config.yaml
```

### macOS

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 main.py --config config/default_config.yaml
```

### Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 main.py --config config/default_config.yaml
```

## Que Se Genera Al Ejecutarlo

Tras lanzar:

```bash
python main.py --config config/default_config.yaml
```

el pipeline crea automaticamente:

- `output/`
- `output/logs/`
- `output/plots/`

No hace falta crear estas carpetas a mano.

## Importante Sobre La Conservacion De Resultados

El pipeline vuelve a generar los outputs cada vez que se ejecuta. Eso significa:

- no se pierde informacion del input original
- si ejecutas otra vez, los outputs del run anterior pueden sobrescribirse
- si quieres conservar historico de una ejecucion concreta, copia o renombra la carpeta `output/` antes de volver a lanzar el pipeline

Ejemplo:

```bash
copy output output_run_2026_04_01
```

o en macOS/Linux:

```bash
cp -R output output_run_2026_04_01
```

## Estructura Del Repositorio

```text
market_basket/
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
|-- .gitignore
|-- main.py
|-- movimientos.xlsx
|-- README.md
`-- requirements.txt
```

## Que Hace Cada Archivo

### Archivos raiz

| Archivo | Para que sirve |
|---|---|
| `main.py` | Punto de entrada. Carga configuracion, inicializa logging y lanza el pipeline completo. |
| `requirements.txt` | Dependencias del proyecto. |
| `README.md` | Guia de uso, ejecucion y entendimiento funcional del repo. |
| `.gitignore` | Evita subir artefactos generados como `output/`. |
| `movimientos.xlsx` | Ejemplo o dataset principal de trabajo local. |

### Configuracion

| Archivo | Para que sirve |
|---|---|
| `config/default_config.yaml` | Parametros del pipeline: rutas, reglas de calidad, thresholds, scoring, performance y formatos de salida. |

### Codigo fuente

| Archivo | Para que sirve |
|---|---|
| `src/market_basket/__init__.py` | Exporta configuracion principal y version del modelo. |
| `src/market_basket/config.py` | Define los dataclasses de configuracion y sus validaciones. |
| `src/market_basket/io.py` | Lee Excel, CSV o Parquet y normaliza nombres de columnas. |
| `src/market_basket/cleaning.py` | Limpieza, exclusiones configurables, calidad de datos, `sku_attributes` y `sku_location_profile`. |
| `src/market_basket/transactions.py` | Construye `transaction_id`, agrega SKU por transaccion y calcula metricas de cesta. |
| `src/market_basket/eda.py` | KPIs, resumen de articulos, resumen de transacciones y series agregadas. |
| `src/market_basket/associations.py` | Coocurrencias, reglas, metricas de pares, item metrics y thresholds adaptativos. |
| `src/market_basket/temporal.py` | Series temporales brutas por periodo y metricas de estabilidad/tendencia. |
| `src/market_basket/scoring.py` | Score final de layout, recomendaciones y columnas explicativas. |
| `src/market_basket/clustering.py` | Grafo SKU-SKU, clusters y hubs operativos. |
| `src/market_basket/similarity.py` | Matriz de similitud para heatmap de top articulos. |
| `src/market_basket/outputs.py` | Escritura de tablas, plots y resumen ejecutivo. |
| `src/market_basket/pipeline.py` | Orquestacion completa del proceso de extremo a extremo. |
| `src/market_basket/utils.py` | Utilidades comunes: normalizacion, escalados, logging y serializacion. |

### Tests

| Archivo | Para que sirve |
|---|---|
| `tests/test_utils.py` | Verifica normalizacion de IDs y strings. |
| `tests/test_cleaning.py` | Verifica reglas de limpieza y perfiles SKU-ubicacion. |
| `tests/test_transactions.py` | Verifica construccion de transacciones y metricas operativas. |
| `tests/test_associations.py` | Verifica calculo de afinidad y metricas como `npmi`. |
| `tests/test_temporal_scoring.py` | Verifica estabilidad temporal y score final. |
| `tests/test_config.py` | Verifica validacion de configuracion. |

## Flujo Del Pipeline

### 1. Ingesta

- lee `input_data`
- detecta formato por extension
- soporta Excel, CSV y Parquet
- normaliza nombres de columnas
- valida que existan las columnas requeridas

### 2. Calidad y limpieza

- parsea fechas y cantidades
- filtra `Tipo movimiento = PI`
- excluye filas sin `Pedido externo` del modelo principal
- puede excluir `quantity <= 0`
- puede eliminar duplicados exactos
- registra exclusiones y riesgos de calidad

### 3. Transacciones

- construye `transaction_id = external_order + separator + owner`
- agrega por transaccion y articulo
- genera cesta binaria y ponderada
- calcula dispersion de ubicaciones y repeticion de SKU

### 4. Afinidad

- frecuencia conjunta
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

### 5. Estabilidad temporal

- historico completo
- ventanas moviles
- años
- trimestres
- slopes y tendencia de soporte/lift

### 6. Scoring de layout

Produce un score final entre `0` y `1` y recomendaciones de accion:

- `candidate_same_slot_area`
- `candidate_same_zone`
- `candidate_manual_review`

### 7. Clustering y hubs

- construccion de grafo de coocurrencia
- deteccion de comunidades
- calculo de hubs
- metricas de densidad y cohesion

## Outputs Generados

### Tablas principales en `output/`

| Archivo | Que contiene | Para que sirve |
|---|---|---|
| `kpi_resumen` | KPIs globales del run | Vista ejecutiva rapida |
| `calidad_datos` | Nulos, exclusiones, incidencias, reglas aplicadas | Control de sesgos y trazabilidad |
| `transacciones_resumen` | Resumen por transaccion | Analisis operativo de cestas |
| `articulos_resumen` | Resumen por SKU | Frecuencia, volumen, hubs, atributos |
| `articulos_por_propietario` | Resumen SKU por propietario | Segmentacion operativa |
| `sku_location_profile` | Perfil de ubicaciones observadas por SKU | Preparar futura fase de layout |
| `item_metrics` | Metricas base por item | Soportes y exclusiones en reglas |
| `afinidad_pares` | Ranking SKU-SKU con score final | Recomendaciones de cercania |
| `afinidad_reglas` | Reglas de asociacion filtradas | Analisis direccional |
| `clusters_sku` | Comunidades de SKUs | Familias operativas |
| `hubs_sku` | Articulos con mayor conectividad | Identificar pivotes operativos |
| `raw_temporal_pairs` | Afinidad por par y periodo sin filtro agresivo | Analisis temporal bruto |
| `temporal_stability_metrics` | Estabilidad, slopes y tendencias | Priorizar relaciones consistentes |
| `series_temporales` | Series agregadas y series de pares | Plots y dashboard |
| `metadata_modelo.json` | Metadata del run, thresholds, mapping de columnas | Auditoria y trazabilidad |
| `resumen_ejecutivo.md` | Hallazgos en lenguaje operativo | Lectura de negocio |

### Logs en `output/logs/`

| Archivo | Para que sirve |
|---|---|
| `pipeline.log` | Trazabilidad del run por etapas |
| `resumen_nulos.csv` | Perfil de nulos por columna |
| `filas_excluidas_sin_pedido_externo.csv` | Filas PI fuera del modelo por no tener pedido externo |

### Visualizaciones en `output/plots/`

| Archivo | Para que sirve |
|---|---|
| `hist_tamano_cesta.png` | Ver tamaño de cesta |
| `top_articulos_frecuencia.png` | Ver SKUs mas frecuentes |
| `heatmap_coocurrencia_top.png` | Ver similitud entre top articulos |
| `grafo_afinidad_skus.png` | Visualizar red de afinidad |
| `evolucion_relaciones_clave.png` | Seguir la evolucion temporal de relaciones importantes |
| `distribucion_scores_layout.png` | Ver la distribucion del score final |

## Configuracion Explicada Parametro A Parametro

La configuracion principal esta en `config/default_config.yaml`.

### Bloque `paths`

| Parametro | Que hace | Ejemplo |
|---|---|---|
| `input_data` | Ruta del fichero de entrada | `movimientos.xlsx` |
| `sheet_name` | Hoja Excel a leer | `Sheet1` |
| `output_dir` | Carpeta base de resultados | `output` |
| `logs_dir` | Carpeta de logs | `output/logs` |
| `plots_dir` | Carpeta de plots | `output/plots` |

### Bloque `columns`

| Parametro | Que representa en el input |
|---|---|
| `movement_type` | Columna de tipo de movimiento |
| `completion_date` | Fecha final de la preparacion/movimiento |
| `article` | Codigo de articulo |
| `article_description` | Descripcion del articulo |
| `quantity` | Cantidad movida |
| `owner` | Propietario |
| `location` | Ubicacion observada |
| `external_order` | Pedido externo |

### Bloque `data_quality`

| Parametro | Que hace | Impacto |
|---|---|---|
| `exclude_non_positive_quantity` | Excluye del modelo filas con cantidad `<= 0` | Evita ruido y relaciones falsas |
| `drop_exact_duplicates` | Elimina duplicados exactos | Limpia dataset si hay duplicacion tecnica |
| `duplicate_subset` | Columnas para detectar duplicados | Define el criterio de duplicidad |

### Bloque `transaction`

| Parametro | Que hace | Valores habituales |
|---|---|---|
| `id_separator` | Separador del `transaction_id` | `|` |
| `date_strategy` | Regla para calcular fecha de transaccion | `max_completion_date`, `min_completion_date`, `mode_date` |

### Bloque `temporal`

| Parametro | Que hace |
|---|---|
| `rolling_windows_days` | Ventanas moviles para estabilidad |
| `include_yearly` | Incluye calculo por año |
| `include_quarterly` | Incluye calculo por trimestre |

### Bloque `thresholds.pairs`

| Parametro | Que hace |
|---|---|
| `min_pair_transactions` | Minimo explicito de transacciones compartidas por par |
| `min_support` | Minimo explicito de soporte conjunto |
| `adaptive_support_floor` | Soporte minimo usado en el ajuste adaptativo |
| `adaptive_pair_count_quantile` | Cuantil usado para fijar minimo adaptativo de recurrencia |
| `adaptive_min_count` | Minimo absoluto del filtro adaptativo |
| `adaptive_max_count` | Tope superior del filtro adaptativo |
| `raw_temporal_min_shared_transactions` | Minimo suave para la serie temporal bruta |

### Bloque `thresholds.rules`

| Parametro | Que hace |
|---|---|
| `min_confidence` | Confianza minima de reglas |
| `min_lift` | Lift minimo de reglas |
| `max_rules_output` | Maximo de reglas exportadas |
| `exclude_frequent_articles_above_support` | Excluye articulos demasiado frecuentes del calculo de reglas |

### Bloque `thresholds.clustering`

| Parametro | Que hace |
|---|---|
| `min_cluster_size` | Tamano minimo de cluster |
| `similarity_threshold` | Score minimo para crear edge en clustering |
| `min_edge_shared_transactions` | Recurrencia minima de edge para clustering |

### Bloque `thresholds.scoring`

| Parametro | Que hace |
|---|---|
| `proximity_bins` | Cortes del score final |
| `proximity_labels` | Etiquetas asociadas a esos cortes |

### Bloque `performance`

| Parametro | Que hace |
|---|---|
| `max_edges_for_clustering` | Limita edges usados en clustering |
| `graph_plot_max_edges` | Limita edges dibujados en el grafo |
| `heatmap_top_n` | Numero de articulos incluidos en el heatmap |
| `raw_temporal_min_period_transactions` | Minimo de transacciones para analizar un periodo |
| `use_sparse_pair_engine` | Reserva para futuro backend disperso |
| `clustering_method` | Metodo de deteccion de comunidades |

### Bloque `model`

| Parametro | Que hace |
|---|---|
| `valid_movement_type` | Valor valido de tipo de movimiento |
| `top_n_articles` | Top articulos usados en tablas y plots |
| `score_weights` | Pesos del score final |
| `popularity_penalty_alpha` | Penalizacion para SKUs demasiado populares |
| `recurrence_penalty_floor` | Suelo de penalizacion por baja recurrencia |
| `key_relationships_to_plot` | Numero de relaciones clave a visualizar |
| `heatmap_metric` | Metrica usada en el heatmap |
| `score_weight_policy` | Que hacer si los pesos no suman 1 |

### Bloque `outputs`

| Parametro | Que hace |
|---|---|
| `write_csv` | Exporta CSV |
| `write_parquet` | Exporta Parquet |
| `write_excel` | Exporta Excel |

### Parametro `log_level`

| Parametro | Que hace |
|---|---|
| `log_level` | Nivel de logging del pipeline |

## Como Cambiar El Entorno De Ejecucion

### Caso 1: quiero usar otro fichero

Cambia:

```yaml
paths:
  input_data: "mi_fichero.xlsx"
```

### Caso 2: quiero leer otra hoja

```yaml
paths:
  sheet_name: "Hoja2"
```

### Caso 3: quiero ser mas estricto con calidad

```yaml
data_quality:
  exclude_non_positive_quantity: true
  drop_exact_duplicates: true
```

### Caso 4: quiero exportar tambien a Excel

```yaml
outputs:
  write_excel: true
```

### Caso 5: quiero conservar un run historico

No cambies el pipeline.
Copia `output/` a otra carpeta antes de lanzar una nueva ejecucion.

## Como Saber Si Ha Ido Bien

Un run correcto deja al menos:

- `output/metadata_modelo.json`
- `output/kpi_resumen.csv` o `.parquet`
- `output/afinidad_pares.csv` o `.parquet`
- `output/clusters_sku.csv` o `.parquet`
- `output/logs/pipeline.log`

Y deberias ver en consola logs por etapas:

- lectura
- limpieza
- transacciones
- EDA
- asociaciones
- temporal
- score y clustering
- escritura de outputs

## Tests

La suite minima incluida usa `unittest`:

```bash
python -m unittest discover -s tests -v
```

## Notas Sobre Outputs Generados y GitHub

El pipeline genera resultados en `output/`, `output/logs/` y `output/plots/`.
Esos ficheros son artefactos generados, no codigo fuente.
Algunos pueden superar el limite de 100 MB de GitHub, por ejemplo:

- `output/raw_temporal_pairs.csv`
- `output/series_temporales.csv`

Por eso `output/` esta en `.gitignore`.
Ignorar `output/` no impide regenerar los resultados; solo evita subirlos por error.

### Caso A: `output/` ya estaba trackeado y quieres quitarlo del indice sin borrar lo local

```bash
git rm -r --cached output
git add .gitignore
git commit -m "Ignore generated outputs"
git push
```

### Caso B: GitHub rechaza el push porque esos archivos grandes ya estaban en un commit local

```bash
git reset HEAD~1
git rm -r --cached output
git add .gitignore
git add .
git commit -m "Initial commit without generated outputs"
git push -f origin main
```

## Limitaciones Conocidas

- la afinidad no equivale por si sola a una recolocacion optima
- aun no se modela distancia fisica real ni restricciones de capacidad
- `Ubicacion` observada puede no equivaler a un maestro unico
- el backend actual de pares esta pensado para claridad y robustez; si el volumen crece mucho, convendra evolucionar a matriz dispersa

## Preparado Para La Siguiente Fase

El proyecto ya deja preparado el cruce futuro con:

- foto de stock
- maestro de ubicaciones
- layout actual
- layout recomendado

Claves de union previstas:

- `article`
- `location`

Outputs especialmente utiles para esa siguiente fase:

- `sku_location_profile`
- `articulos_resumen`
- `afinidad_pares`
- `clusters_sku`
- `hubs_sku`

## Dataset Del Repo Ya Analizado

En el dataset actual trabajado en este repo:

- 291.237 filas totales
- 66.336 movimientos `PI`
- 63.378 filas validas para el modelo principal
- 21.120 transacciones validas
- 7.847 SKUs unicos
- 74 propietarios
- 60,59% de transacciones con un solo articulo

Eso te da una referencia de densidad real del problema y de por que este proyecto esta orientado a uso logistico real y no a un ejemplo academico simplificado.
