# Rentabilidad 2026 Mahou

## Qué se ha comparado

- Base operativa: layout `rotacion_2026` ya validado.
- Escenario A: 12 pasillos a retráctil.
- Escenario B: escenario mixto de compromiso con pasillos 11,12 a articulada.
- Sensibilidad adicional: convertir de 1 a 4 pasillos finales para ver cuándo empieza a compensar solo por capacidad.

## Hallazgos clave

- El layout `rotacion_2026` reduce el paseo proxy anual de 65.74 h a 38.37 h.
- Eso equivale a 27.37 h/año y 602.23 EUR/año de ahorro laboral directo solo por cercanía a expedición.
- La concentración mejora de 10.14 pasillos medios por propietario a 1.55, y el span medio baja de 10.84 a 0.63 pasillos.
- El propietario que más ahorro captura es 23 MAHOU-MARKETING OCIO, con 153.08 EUR/año proxy.
- Quedan fuera del layout de detalle 241 líneas 2026 de los propietarios: 40 MARKETING INNOVACION JOSERRA, 16 MAHOU-CRISTINA MENENDEZ, 87 DCMC - ANA BELEN RUIZ. No se les ha imputado ahorro artificial.

## Escenario A: todo retráctil

- Coste equipo de referencia por máquina: 7173.00 EUR/año.
- No crea incompatibilidades AM/TR en altura.
- Mantiene la lógica operativa de poner delante lo que más rota ahora.
- Es la opción más limpia para ahorrar tiempo sin reabrir el problema tipológico.

## Escenario B: mixto 11-12 articulada

- Prima de equipo frente a reach: 6869.52 EUR/año por máquina.
- Capacidad extra teórica: 69.68 posiciones equivalentes.
- Valor anual de esa capacidad si se llenase al 100% sustituyendo externo tipo EUR: 5184.00 EUR/año.
- Pero deja 93.91 posiciones equivalentes AM/TR sin encaje natural en altura.
- Resultado: la flexibilidad neta cae a -24.23 posiciones equivalentes.

## Lectura de negocio

- Si solo miras almacenamiento teórico, abrir más pasillos articulados mejora la foto económica.
- El mejor caso “sobre el papel” es 4 pasillos articulados (9,10,11,12), con 3453.70 EUR/año antes de castigar AM/TR.
- Pero ese mismo caso deja 338.39 posiciones AM/TR problemáticas y destruye 199.04 posiciones equivalentes netas de flexibilidad.

## Recomendación

- Recomendación operativa y económica: **todo retráctil**.
- Motivo: el ahorro por cercanía y concentración ya aparece con `rotacion_2026`, mientras que la articulada solo empieza a defenderse si monetizas muchísima capacidad adicional, pero justo los pasillos finales concentran stock con necesidad AM/TR y eso invalida gran parte del supuesto.
- Solo tendría sentido abrir articulada si primero rediseñas la tipología de los propietarios de cola o aceptas que parte del beneficio de capacidad se te va a ir en spillover a pasillos reach.
