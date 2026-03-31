# Market Basket para Layout de Picking

Proyecto Python orientado a operaciones logísticas para analizar afinidad entre artículos de almacén y traducirla a recomendaciones de proximidad física en el layout de picking.

## Por qué la transacción correcta es `Pedido externo + Propietario`

En este dataset un mismo `Pedido externo` puede aparecer con varios `Propietario`. Al perfilar el Excel se detectaron **2.268 pedidos externos compartidos por más de un propietario**. Si agrupáramos solo por `Pedido externo`, mezclaríamos recorridos físicamente distintos y el número de transacciones quedaría infravalorado en torno a **20,5%**. Por eso la unidad correcta para el modelo es:

`transaction_id = Pedido externo + Propietario`

## Qué hace el pipeline

1. Lee el Excel y valida columnas requeridas.
2. Filtra solo `Tipo movimiento = PI`.
3. Limpia fechas, cantidades, nulos, duplicados potenciales e inconsistencias básicas.
4. Construye transacciones binarias y ponderadas por cantidad.
5. Genera EDA operativo.
6. Calcula afinidad SKU-SKU con métricas clásicas y métricas orientadas a layout.
7. Evalúa estabilidad temporal en histórico, ventanas móviles, años y trimestres.
8. Calcula un score final de cercanía para layout.
9. Genera clusters, hubs, tablas desacopladas para dashboard, visualizaciones y resumen ejecutivo.

## Estructura

```text
.
├── config/
│   └── default_config.yaml
├── src/
│   └── market_basket/
│       ├── __init__.py
│       ├── associations.py
│       ├── cleaning.py
│       ├── clustering.py
│       ├── config.py
│       ├── eda.py
│       ├── io.py
│       ├── outputs.py
│       ├── pipeline.py
│       ├── scoring.py
│       ├── similarity.py
│       ├── temporal.py
│       ├── transactions.py
│       └── utils.py
├── main.py
└── requirements.txt
```

## Instalación

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Ejecución

```bash
python main.py --config config/default_config.yaml
```

## Salidas principales

En `output/` se generan, como mínimo:

- `kpi_resumen`
- `calidad_datos`
- `transacciones_resumen`
- `articulos_resumen`
- `afinidad_pares`
- `afinidad_reglas`
- `clusters_sku`
- `hubs_sku`
- `articulos_por_propietario`
- `series_temporales`
- `metadata_modelo.json`
- `resumen_ejecutivo.md`

En `output/logs/`:

- `pipeline.log`
- `resumen_nulos.csv`
- `filas_excluidas_sin_pedido_externo.csv`

En `output/plots/`:

- histograma de tamaño de cesta
- top artículos por frecuencia
- heatmap de afinidad top SKUs
- grafo de afinidad
- evolución temporal de relaciones clave
- distribución del score final

`series_temporales` combina dos tipos de serie:

- evolución agregada de transacciones por año, trimestre, mes y distribución de tamaño de cesta
- evolución temporal de afinidad por pares SKU-SKU

## Lógica del modelo

### Afinidad por pares

Para cada par SKU-SKU se calculan:

- frecuencia conjunta
- soporte conjunto
- confianza A->B y B->A
- lift
- leverage
- conviction
- Jaccard
- cosine
- coocurrencia ponderada por cantidad

### Estabilidad temporal

Cada relación se recalcula en:

- histórico completo
- últimos 365 días
- últimos 180 días
- últimos 90 días
- por año
- por trimestre

Con ello se obtiene un `temporal_stability_score` que combina:

- cuántos periodos aparece la relación
- variación del soporte entre periodos
- variación del lift entre periodos

### Score final de layout

```text
score = (w_freq * frecuencia_normalizada
       + w_lift * lift_normalizado
       + w_conf * balanced_confidence
       + w_similarity * similitud
       + w_stability * estabilidad_temporal
       + w_volume * volumen_ponderado)
       * penalizacion_por_baja_recurrencia
       * penalizacion_por_sku_excesivamente_popular
```

Los pesos se ajustan en `config/default_config.yaml`.

## Hallazgos iniciales del dataset usado

- 291.237 filas totales.
- 66.336 movimientos `PI`.
- 63.378 filas válidas para el modelo principal.
- 21.120 transacciones válidas.
- 7.847 SKUs únicos.
- 74 propietarios.
- 60,59% de transacciones con un solo artículo.
- 31,08% de SKUs aparecen en más de una ubicación observada, por lo que la ubicación se trata como atributo operativo observado y no como maestro único.
- No aparecen movimientos válidos en 2025 en el histórico disponible, algo que conviene revisar operativamente.
