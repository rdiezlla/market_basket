# Resumen ejecutivo automático

## KPIs principales
- Movimientos PI válidos para modelo: 63378
- Transacciones válidas: 21120
- SKUs únicos: 7847
- Propietarios únicos: 74
- Cesta media: 2.42

## Relaciones con mayor recomendación de cercanía
- 022009 + 031006: score 0.721, frecuencia 93, lift 91.60
- 014012 + 014014: score 0.668, frecuencia 248, lift 13.88
- 023080 + 031124: score 0.572, frecuencia 48, lift 405.67
- 011026 + 014012: score 0.543, frecuencia 132, lift 7.96
- 087002 + 087004: score 0.530, frecuencia 81, lift 69.09
- 065072 + 126105: score 0.526, frecuencia 402, lift 28.35
- 022043 + 031045: score 0.523, frecuencia 92, lift 93.17
- 016010 + 061016: score 0.516, frecuencia 73, lift 14.64
- 022053 + 032040: score 0.511, frecuencia 107, lift 51.97
- 022043 + 031047: score 0.506, frecuencia 101, lift 91.02

## Relaciones muy fuertes pero menos frecuentes
- 124229 + 124230: lift 4224.00, frecuencia 5, score 0.075
- 124228 + 124230: lift 4224.00, frecuencia 5, score 0.074
- 124228 + 124229: lift 4224.00, frecuencia 5, score 0.074
- 126107 + 126108: lift 4224.00, frecuencia 5, score 0.072
- 205002 + 205029: lift 4224.00, frecuencia 5, score 0.071
- 205002 + 205030: lift 4224.00, frecuencia 5, score 0.071
- 205002 + 205031: lift 4224.00, frecuencia 5, score 0.071
- 205029 + 205030: lift 4224.00, frecuencia 5, score 0.071
- 205029 + 205031: lift 4224.00, frecuencia 5, score 0.071
- 205030 + 205031: lift 4224.00, frecuencia 5, score 0.071

## Relaciones frecuentes y estables
- 014012 + 014014: estabilidad 0.88, frecuencia 248, score 0.668
- 022009 + 031006: estabilidad 0.83, frecuencia 93, score 0.721
- 023080 + 031124: estabilidad 0.83, frecuencia 48, score 0.572
- 016010 + 061016: estabilidad 0.83, frecuencia 73, score 0.516
- 012012 + 167006: estabilidad 0.83, frecuencia 56, score 0.438
- 022009 + 204000: estabilidad 0.80, frecuencia 52, score 0.451
- 011026 + 014012: estabilidad 0.79, frecuencia 132, score 0.543
- 031006 + 204000: estabilidad 0.75, frecuencia 47, score 0.380
- 014012 + 016010: estabilidad 0.75, frecuencia 57, score 0.375
- 014012 + 061016: estabilidad 0.75, frecuencia 60, score 0.431

## Clusters operativos destacados
- Cluster 12: tamaño 2, cohesión 0.572, artículos: 023080, 031124
- Cluster 13: tamaño 2, cohesión 0.530, artículos: 087002, 087004
- Cluster 14: tamaño 2, cohesión 0.511, artículos: 022053, 032040
- Cluster 9: tamaño 3, cohesión 0.455, artículos: 013069, 013070, 013071
- Cluster 15: tamaño 2, cohesión 0.430, artículos: 1836BR, 74P

## Hubs de la red de afinidad
- 014012: grado 11, peso acumulado 4.51
- 014014: grado 11, peso acumulado 3.65
- 065072: grado 11, peso acumulado 3.44
- 126105: grado 10, peso acumulado 3.15
- 012012: grado 9, peso acumulado 3.14

## Riesgos y sesgos de datos
- missing_external_order: 129744 filas. Filas sin pedido externo.
- missing_location: 124561 filas. Filas sin ubicación.
- non_positive_quantity: 70353 filas. Filas con cantidad nula o no positiva.
- rows_after_movement_filter: 66336 filas. Filas conservadas tras filtrar Tipo movimiento = PI.
- missing_article: 64214 filas. Filas sin artículo.
- missing_owner: 64214 filas. Filas sin propietario.
- rows_after_model_filters: 63378 filas. Filas válidas para el modelo principal.
- potential_duplicate_rows: 12732 filas. Filas duplicadas sobre la clave operativa mínima.