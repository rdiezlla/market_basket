# beneficio_layout_actual_2026_v2

## 1. por que el modelo anterior era conservador

- La v1 convertia sobre todo metros en horas y, por tanto, monetizaba poco del beneficio real de agrupar por propietario.
- La v1 medía bien la dispersion espacial, pero no hacia visible el castigo operativo de los cambios de pasillo, la fragmentacion del propietario, la maniobra, la busqueda y el reenganche de ruta.
- La v2 mantiene el hecho observado y el contrafactual, pero usa un modelo de tiempo operativo con drivers explicitamente parametrizados y calibrado contra tiempos reales observados en 2026.

## 2. que parte del repo se reutilizo

- Se han reutilizado como base estructural: main.py, src/market_basket/pipeline.py, src/market_basket/cleaning.py, src/market_basket/transactions.py, src/market_basket/eda.py, src/market_basket/outputs.py, src/market_basket/mahou_dimensioning.py, output/transacciones_resumen.csv, output/sku_location_profile.csv.
- Se ha mantenido la logica actual vs contrafactual del almacen actual 2026.
- La nueva pieza se limita a ampliar el modelo operativo y economico, sin romper la v1.

## 3. como esta construido el repo actualmente

- `main.py` sigue siendo el entrypoint del pipeline general de market basket.
- `transactions.py` aporta la unidad `pedido externo + propietario`, que sigue siendo la unidad principal de este caso.
- `mahou_beneficio_layout_actual_2026.py` queda conservado como referencia v1.
- `mahou_beneficio_layout_actual_2026_v2.py` es la capa nueva para decision operativa de gerente de plataforma.

## 4. como se modelo el almacen actual

- Hecho observado: foto real `17-04-2026.xlsx` y movimientos `PI` 2026 del almacen actual.
- Geometria fija del modelo: 5.50 m por salto entre pasillos contiguos y 1.20 m por posicion longitudinal.
- Tiempo observado real: se usa la duracion de las lineas de movimiento 2026 winsorizada para reducir outliers extremos sin ocultarlos.
- El tiempo actual de referencia del modelo v2 se ancla al tiempo observado, no a una velocidad teorica de paseo.

## 5. como se construyo el layout contrafactual

- Cada propietario se agrupa en el minimo numero razonable de pasillos contiguos dentro del almacen actual.
- La capacidad por pasillo se mantiene tomada de la foto real de stock.
- En el escenario recomendado se prioriza hacia expedicion lo que mas rota en 2026.
- La simulacion no cambia de nave ni usa el layout destino.

## 6. ahorro en metros

- Escenario base recomendado: 255,179.40 m.
- Escenario conservador: 215,513.60 m.
- Escenario agresivo: 265,194.40 m.

## 7. ahorro en horas

- Escenario base recomendado: 250.93 h.
- El tiempo ya no sale solo de metros / velocidad: incorpora cambios de pasillo, stops, fragmentacion, bloques discontinuos, busqueda, maniobra y reenganche de ruta.
- Horas actuales para el volumen 2026 observado: 397.22 h.
- Horas contrafactuales para el mismo volumen: 146.28 h.

## 8. ahorro equivalente, realizable y variable

- Ahorro equivalente base recomendado: 7,245.72 EUR.
- Ahorro realizable directo base recomendado: 0.00 EUR.
- Coste variable evitable base recomendado: 6,022.42 EUR.
- Ahorro equivalente != ahorro realizable.
- Si no se elimina una persona o una carretilla entera, el valor sigue existiendo como horas evitadas, menor necesidad de extra/refuerzo y capacidad adicional.

## 9. productividad y capacidad

- Transacciones/hora actual: 2.83
- Transacciones/hora contrafactual: 7.68
- Lineas/hora actual: 9.73
- Lineas/hora contrafactual: 26.42
- Uplift de productividad lineas/hora: 171.5%
- Capacidad adicional con la misma dotacion: 171.5%
- Pedidos adicionales absorbibles con la misma dotacion: 1,928.11
- Lineas adicionales absorbibles con la misma dotacion: 6,630.02

## 10. personas equivalentes y carretillas equivalentes

- Personas equivalentes base: 0.1426
- Personas realizables base: 0
- Carretillas equivalentes base: 0.1307
- Carretillas realizables base: 0
- El ahorro realizable de carretilla solo se presenta en enteros completos.

## 11. de donde viene el beneficio operativo

- menos_metros: 182.35 h
- menos_cambios_pasillo: 21.40 h
- menos_fragmentacion_propietario: 15.12 h
- menos_maniobra: 12.39 h
- menos_busqueda_reorientacion: 8.81 h
- menos_reenganche_ruta: 7.31 h
- menos_bloques_discontinuos: 3.04 h
- menos_stops: 0.50 h

## 12. sensibilidad, riesgos y conclusion ejecutiva

- Escenario recomendado: B_base_recomendado.
- Maximo ahorro razonable del set de sensibilidad: 7,571.82 EUR equivalentes.
- Riesgos principales: la foto de stock es un corte, la reasignacion contrafactual sigue siendo una simulacion, y parte del beneficio puede capturarse como capacidad y no como baja directa.
- Conclusion ejecutiva: agrupar por propietario en el almacen actual mejora de forma visible la productividad real porque reduce la dispersion operativa y no solo la distancia recorrida.

ahorro base recomendado: 7,245.72 EUR equivalentes, 250.93 h, 255,179.40 m
maximo ahorro razonable: 7,571.82 EUR equivalentes, 262.23 h, 265,194.40 m
que supuesto cambia mas el resultado: la severidad de penalizacion de fragmentacion/cambios de pasillo junto con la secuenciacion contrafactual dentro del bloque del propietario
