# Rentabilidad de centro 2026

## Qué se ha medido

- Base real: movimientos `PI` de `2026-01-01` a `2026-04-20`.
- El análisis no mide ahorro “de un propietario”, sino ahorro total del centro por quitar dispersión entre pasillos dentro del mismo propietario.
- Se han comparado tres estados:
  - actual disperso
  - actual compacto por propietario dentro del almacén actual
  - layout `rotacion_2026`

## Cómo se ha monetizado

- Se han detectado batches reales `operario + día + propietario`.
- Para cada batch se ha calculado el salto actual entre pasillos y el salto mínimo si el propietario estuviera compacto.
- La calibración observada da una mediana de 101.0 segundos extra al cambiar de pasillo respecto a quedarse en el mismo.
- El escenario base monetiza cada rank de salto con `60` segundos, y además se deja sensibilidad a `45, 60, 75, 90` segundos.

## Resultado para gerencia

- Horas PI anualizadas actuales observadas: 1506.12 h/año.
- Si compactas el almacén actual por propietario: recuperas 84.39 h/año, equivalentes a 1856.63 EUR/año de personal.
- Si vas al layout `rotacion_2026`: recuperas 105.35 h/año, equivalentes a 2317.75 EUR/año de personal.

## Lectura de personal y carretillas

- En equivalente de plantilla, `rotacion_2026` libera 0.060 FTE.
- En equivalente de máquina, `rotacion_2026` libera 0.055 carretillas-año.
- A coste de reach, eso equivale a 724.30 EUR/año.
- A coste de articulada, eso equivale a 987.68 EUR/año.
- Con estos datos de `PI` no se justifica por sí solo eliminar una persona completa ni una carretilla completa; el ahorro es sobre todo de productividad recuperable y capacidad operativa redeplegable.

## Dónde está el ahorro

- El propietario que más aporta al ahorro total es 4 SOLAN, con 486.67 EUR/año.
- El operario con más tiempo potencialmente liberado en este flujo es 240, con 498.83 EUR/año equivalentes.
- El foco del ahorro viene de pocos propietarios muy rotadores y muy dispersos: `4`, `23`, `3`, `95`, `30`, `29`.

## Conclusión

- Sí hay ahorro claro al agrupar por propietario.
- La cifra base defendible con el fichero de movimientos es del orden de 1857 a 2318 EUR/año de personal directo, más 724 EUR/año de capacidad-equivalente de reach si esa productividad te permite reducir uso efectivo de máquina.
- La mejora es real, pero el propio fichero `PI` acota el techo: en todo el periodo solo se observan 453.90 h directas de este flujo, así que no sería honesto prometer un ahorro estructural enorme de plantilla sin más datos de otras tareas.
