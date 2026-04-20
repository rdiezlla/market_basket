# 1. qué se recalculó

- Se recalculó la ocupación actual desde `17-04-2026.xlsx` consolidando a unidad física ocupada y separando `posicion_suelo`, `subhueco_balda`, `modulo_3eu` y `eu_equivalente`.
- Se reconstruyó el ranking operativo por propietario desde `movimientos.xlsx`, con `lineas_solicitudes_con_pedidos.xlsx` como sanity check secundario.
- Se incorporó `STOCK_MAP_FACT_MAHOU_260415020005.xlsx` con conversión por capas A/B/C y validación explícita de granularidad.

# 2. qué cifras del informe avanzado se corrigieron

- `suelo_250`: el avanzado mezcla `252 posiciones`, `121 módulos` y `84 módulos`; Codex recalcula 246 `posicion_suelo`, 249.5 `eu_equivalente` y 83.17 `modulo_3eu`.
- `suelo_300`: Codex recalcula 120 `posicion_suelo`, 122.0 `eu_equivalente` y 40.67 `modulo_3eu`.
- `suelo_126`: el avanzado mezcla `101 posiciones`, `40 módulos` y `34 módulos`; Codex recalcula 101 `posicion_suelo` y 33.67 `modulo_3eu`.
- `balda_9h`: el avanzado mezcla `540 subhuecos`, `557 módulos equivalentes` y `180 módulos`; Codex recalcula 536 `subhueco_balda`, 59.56 `eu_equivalente` y 19.85 `modulo_3eu`.

# 3. qué está decisión-grade hoy

- Capacidad teórica destino: 6480.0 EU-equivalente.
- Penalización recalculada de altura 10: 340.8 EU-equivalente.
- Demanda base con externo soportado: 5623.0 EU-equivalente.
- Gap contra diseño 90%: 97.7 EU-equivalente por cerrar o absorber con buffer.
- Con diseño al 90%, no cabe con el soporte actual.

# 4. qué sigue condicionado por stock externo

- Granularidad externo: granularidad heterogénea con riesgo alto.
- Ratio ADR repetido: 62.97%. Ratio QCNT=UDS: 63.98%.
- Todo lo inferido por capa C se deja como condicionado y la revisión manual se mantiene fuera del cierre duro de layout.

# 5. layout base recomendado

- Propietario 23: pasillo principal 1, secundario sin_hueco_soportado, dedicado.
- Propietario 3: pasillo principal 1, secundario 2, split_contiguo.
- Propietario 24: pasillo principal 4, secundario 5, split_contiguo.
- Propietario 4: pasillo principal 5, secundario 6, split_contiguo.
- Propietario 29: pasillo principal 6, secundario 7, split_contiguo.
- Propietario 82: pasillo principal 7, secundario sin_hueco_soportado, dedicado.
- Propietario 5: pasillo principal 7, secundario 8, split_contiguo.
- Propietario 30: pasillo principal 8, secundario sin_hueco_soportado, dedicado.
- Propietario 33: pasillo principal 8, secundario 9, split_contiguo.
- Propietario 61: pasillo principal 9, secundario 10, split_contiguo.
- Propietario 89: pasillo principal 9, secundario sin_hueco_soportado, dedicado.
- Propietario 14: pasillo principal 9, secundario sin_hueco_soportado, dedicado.

# 6. layout condicionado

- Propietario 23: pasillo principal 1, secundario sin_hueco_soportado, dedicado.
- Propietario 3: pasillo principal 1, secundario 2, split_contiguo.
- Propietario 24: pasillo principal 5, secundario 6, split_contiguo.
- Propietario 4: pasillo principal 6, secundario 7, split_contiguo.
- Propietario 29: pasillo principal 7, secundario 8, split_contiguo.
- Propietario 82: pasillo principal sin_hueco_soportado, secundario sin_hueco_soportado, split_contiguo.
- Propietario 5: pasillo principal sin_hueco_soportado, secundario sin_hueco_soportado, split_contiguo.
- Propietario 30: pasillo principal sin_hueco_soportado, secundario sin_hueco_soportado, split_contiguo.
- Propietario 33: pasillo principal sin_hueco_soportado, secundario sin_hueco_soportado, split_contiguo.
- Propietario 14: pasillo principal sin_hueco_soportado, secundario sin_hueco_soportado, split_contiguo.
- Propietario 62: pasillo principal sin_hueco_soportado, secundario sin_hueco_soportado, split_contiguo.
- Propietario 89: pasillo principal sin_hueco_soportado, secundario sin_hueco_soportado, split_contiguo.

# 7. necesidades mínimas de infra

- balda_9h_actual: 19.85 módulos 3EU, 59.56 EU-equivalente, impacto_10=0.00.
- suelo_126_actual: 33.67 módulos 3EU, 101.00 EU-equivalente, impacto_10=0.00.
- suelo_250_actual: 83.17 módulos 3EU, 249.50 EU-equivalente, impacto_10=249.50.
- suelo_300_actual: 40.67 módulos 3EU, 122.00 EU-equivalente, impacto_10=122.00.

# 8. riesgos y siguiente dato crítico a validar

- granularidad_stock_externo: ADR repetido=62.97%; QCNT=UDS=63.98%
- conciliacion_entre_estudios: 40 filas de conciliación con diferencias comentadas
- unidades_tipologias_especiales: posicion_suelo, subhueco_balda, modulo_3eu y eu_equivalente separados
- penalizacion_altura_10: solo penalizan suelo_250 y suelo_300; slots afectados=366
- doble_conteo_stock_actual_vs_externo: sin clave física común; solape físico directo no demostrado; ratio dummy=0.00%
- propietarios_que_cambian_de_bloque: propietarios con cambio de pasillo principal al incluir inferido=35