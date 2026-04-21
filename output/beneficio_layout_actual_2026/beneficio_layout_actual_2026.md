# beneficio_layout_actual_2026

## 1. cómo está construido el repo actualmente

- El repo está organizado alrededor de un pipeline general de market basket con limpieza, transacciones, EDA, asociaciones, scoring y outputs.
- La parte base sí genera métricas de dispersión transaccional (`unique_locations_in_basket`, `basket_dispersion_proxy`) y perfiles SKU-localización.
- No existía una simulación espacial contrafactual del almacén actual agrupado por propietario; esa pieza se ha añadido en este trabajo.

## 2. qué parte del repo se reutilizó

- Se reutilizaron principalmente los conceptos y salidas de `transactions.py`, `cleaning.py`, `outputs.py` y la normalización de `mahou_dimensioning.py`.
- Módulos reutilizados de forma directa o parcial: main.py, src/market_basket/pipeline.py, src/market_basket/cleaning.py, src/market_basket/transactions.py, src/market_basket/eda.py, src/market_basket/outputs.py, src/market_basket/mahou_dimensioning.py, output/transacciones_resumen.csv, output/sku_location_profile.csv.
- No se reutilizó la lógica del almacén destino porque este caso es exclusivamente del almacén actual.

## 3. cómo se modeló el almacén actual

- Hecho observado base: foto de stock `17-04-2026.xlsx` + movimientos `PI` de 2026.
- Geometría usada: 5,50 m por cambio lateral entre pasillos contiguos y 1,20 m por posición longitudinal.
- Cada localización actual se convirtió en coordenadas `(x,y)` usando pasillo y columna reales.
- El layout actual por propietario se midió con la foto de stock ocupada de la nave actual, no con el almacén destino.

## 4. cómo se construyó el layout contrafactual

- Cada propietario se reasignó a un bloque continuo de pasillos dentro de la nave actual, respetando la capacidad física observada por pasillo.
- La capacidad requerida por propietario se tomó de su footprint ocupado en la foto de stock.
- En el escenario base, los propietarios con más `picks_2026` se acercan antes a expedición.
- Dentro del bloque de cada propietario, las localizaciones más usadas en 2026 se colocan primero.

## 5. ahorro en metros

- Escenario base recomendado: de 321,610.00 m a 66,430.60 m, con un ahorro total de 255,179.40 m.
- Escenario conservador: 215,513.60 m.
- Escenario agresivo: 265,187.20 m.

## 6. ahorro en horas

- Escenario base recomendado: 59.07 h.
- Escenario conservador: 39.91 h.
- Escenario agresivo: 81.85 h.
- Métrica principal recomendada: `transacción (pedido externo + propietario)` porque el picking se lanza por propietario y esa unidad capta mejor el recorrido real que la línea aislada.

## 7. ahorro en euros

- Escenario base recomendado: 1,299.52 EUR de personal y 406.10 EUR equivalentes de carretilla.
- Ahorro total equivalente base: 1,705.63 EUR.
- Máximo ahorro razonable del set de sensibilidad: 2,363.36 EUR.

## 8. personas equivalentes

- Escenario base recomendado: 0.0336 personas equivalentes.
- Esto refleja ahorro potencial teórico de capacidad, no necesariamente una baja real de plantilla.

## 9. carretillas equivalentes

- Escenario base recomendado: 0.0308 carretillas equivalentes frente a una dotación actual de 3.
- El ahorro realizable de carretilla sigue siendo 0.00 EUR porque no se llega a liberar una carretilla completa.

## 10. sensibilidad

- La sensibilidad se ha construido con tres escenarios: conservador, base recomendado y agresivo.
- Cambian tres cosas: orden de asignación por propietario, lógica interna de mapeo de localizaciones y velocidad efectiva de conversión metros->tiempo.
- La tabla `tabla_sensibilidad.csv` deja trazado qué parte del resultado depende de cada supuesto.

## 11. riesgos y limitaciones

- La foto de stock es una instantánea; la ocupación simultánea real cambia a lo largo de 2026.
- El contrafactual no cambia de nave ni inventa nuevas geometrías, pero sí supone que el re-slotting por propietario es ejecutable dentro de la capacidad observada.
- El ahorro realizable de personal o carretilla solo aparece si la organización adapta la dotación; si no, el ahorro queda como productividad absorbida.
- El escenario agresivo ya incorpora un plus operativo por mejor secuenciación dentro del bloque del propietario.

## 12. conclusión ejecutiva

- Con la información disponible, sí habría existido un ahorro real si el almacén actual de 2026 hubiera estado agrupado por propietario.
- El ahorro principal viene de reducir saltos entre pasillos dentro del mismo propietario, no de afinidad SKU-SKU.
- La conclusión robusta es que el beneficio existe en recorrido, tiempo y coste equivalente, aunque la captura realizable depende de si la operación ajusta recursos.

ahorro base recomendado: 1,705.63 EUR equivalentes (59.07 h; 255,179.40 m)
máximo ahorro razonable: 2,363.36 EUR equivalentes (81.85 h; 265,187.20 m)
qué supuesto cambia más el resultado: la combinación de `velocidad efectiva` y `criterio de secuenciación contrafactual` dentro del bloque del propietario
