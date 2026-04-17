# Resumen ejecutivo automatico

## KPIs principales
- Movimientos PI validos para modelo: 64032
- Transacciones validas: 21306
- SKUs unicos: 7962
- Propietarios unicos: 75
- Cesta media: 2.42

## Oportunidades prioritarias de cercania
- 022009 + 031006: score 0.671, frecuencia 93, recomendacion very_high, accion sugerida: Priorizar cercania fisica fuerte dentro de la misma area o frente de picking.
- 014012 + 014014: score 0.663, frecuencia 249, recomendacion very_high, accion sugerida: Priorizar cercania fisica fuerte dentro de la misma area o frente de picking.
- 087002 + 087004: score 0.600, frecuencia 81, recomendacion high, accion sugerida: Mantener dentro de la misma zona operativa y revisar secuencia de recorrido.
- 023080 + 031124: score 0.588, frecuencia 48, recomendacion high, accion sugerida: Mantener dentro de la misma zona operativa y revisar secuencia de recorrido.
- 012012 + 012019: score 0.568, frecuencia 129, recomendacion high, accion sugerida: Mantener dentro de la misma zona operativa y revisar secuencia de recorrido.
- 022043 + 031045: score 0.556, frecuencia 92, recomendacion high, accion sugerida: Mantener dentro de la misma zona operativa y revisar secuencia de recorrido.
- 022043 + 031047: score 0.547, frecuencia 101, recomendacion high, accion sugerida: Mantener dentro de la misma zona operativa y revisar secuencia de recorrido.
- 065072 + 084099: score 0.531, frecuencia 239, recomendacion high, accion sugerida: Mantener dentro de la misma zona operativa y revisar secuencia de recorrido.
- 065072 + 126105: score 0.523, frecuencia 402, recomendacion high, accion sugerida: Mantener dentro de la misma zona operativa y revisar secuencia de recorrido.
- 022053 + 032040: score 0.520, frecuencia 107, recomendacion high, accion sugerida: Mantener dentro de la misma zona operativa y revisar secuencia de recorrido.

## Relaciones fuertes pero con baja recurrencia
- 155006 + 155007: lift 4261.20, frecuencia 5, revision manual True
- 126107 + 126108: lift 4261.20, frecuencia 5, revision manual True
- 205202 + 205203: lift 4261.20, frecuencia 5, revision manual True
- 086103 + 086104: lift 4261.20, frecuencia 5, revision manual True
- 205002 + 205029: lift 4261.20, frecuencia 5, revision manual True
- 205002 + 205030: lift 4261.20, frecuencia 5, revision manual True
- 205002 + 205031: lift 4261.20, frecuencia 5, revision manual True
- 205029 + 205030: lift 4261.20, frecuencia 5, revision manual True
- 205029 + 205031: lift 4261.20, frecuencia 5, revision manual True
- 205030 + 205031: lift 4261.20, frecuencia 5, revision manual True

## Relaciones emergentes
- 087002 + 087004: score 0.600, frecuencia 81, tendencia soporte growing, tendencia lift declining
- 022043 + 031047: score 0.547, frecuencia 101, tendencia soporte declining, tendencia lift growing
- 065072 + 084099: score 0.531, frecuencia 239, tendencia soporte stable, tendencia lift growing
- 065072 + 126105: score 0.523, frecuencia 402, tendencia soporte growing, tendencia lift declining
- 022053 + 032040: score 0.520, frecuencia 107, tendencia soporte declining, tendencia lift growing
- 016010 + 061016: score 0.514, frecuencia 74, tendencia soporte stable, tendencia lift growing
- 013070 + 013071: score 0.487, frecuencia 60, tendencia soporte declining, tendencia lift growing
- 011104 + 011105: score 0.480, frecuencia 131, tendencia soporte declining, tendencia lift growing
- 013069 + 013071: score 0.469, frecuencia 58, tendencia soporte declining, tendencia lift growing
- 013069 + 013070: score 0.468, frecuencia 58, tendencia soporte declining, tendencia lift growing

## Clusters operativos destacados
- Cluster 16: tamano 2, densidad 1.00, score medio 0.520, lift medio 52.43, articulos: 022053, 032040
- Cluster 10: tamano 3, densidad 1.00, score medio 0.474, lift medio 347.24, articulos: 013069, 013070, 013071
- Cluster 2: tamano 13, densidad 0.22, score medio 0.373, lift medio 81.39, articulos: 022009, 022043, 023080, 031006, 031045, 031047, 031124, 201026
- Cluster 4: tamano 11, densidad 0.36, score medio 0.339, lift medio 35.55, articulos: 012012, 012019, 012037, 012040, 012053, 014024, 063014, 087002
- Cluster 17: tamano 2, densidad 1.00, score medio 0.325, lift medio 84.46, articulos: 087037, 141136

## SKUs hub operativos
- 014012: grado 15, peso acumulado 5.55, betweenness 0.002
- 014014: grado 12, peso acumulado 4.06, betweenness 0.028
- 012012: grado 11, peso acumulado 3.77, betweenness 0.038
- 065072: grado 11, peso acumulado 3.61, betweenness 0.003
- 011026: grado 11, peso acumulado 3.35, betweenness 0.015

## Riesgos y sesgos de datos
- rows_after_movement_filter: 67332 filas. Rows kept after filtering movement_type = PI.
- rows_after_model_filters: 64032 filas. Rows available for the main basket model after cleaning and business filters.
- missing_external_order: 63958 filas. Rows without external order.
- missing_location: 17350 filas. Rows without location.
- potential_duplicate_rows: 3318 filas. Potential duplicates detected over the configured subset.
- rows_excluded_missing_external_order_in_pi: 3300 filas. PI rows excluded from the main model due to missing external order.
- non_positive_quantity_raw: 1 filas. Rows with null, zero or negative quantity.
- missing_article: 0 filas. Rows without article.