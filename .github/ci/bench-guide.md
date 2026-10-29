<details><summary>📖 Cómo leer estos resultados</summary>

**Para qué sirve.** Detectar si este PR hace qsrv más lento o hace que reserve más memoria que `main`. Los números absolutos importan poco: con ~0,6 msg/s de carga real hay margen de miles de mensajes/s. Lo que importa es la **diferencia entre base y head**.

**Las tablas de benchstat.** Cada tabla es una métrica; cada fila, un benchmark. Hay una columna `base` (main), una `head` (este PR) y una `vs base` con la diferencia.

| métrica | qué es | mejor si |
|---|---|---|
| `sec/op` | tiempo por operación (`µs` = microsegundos, `ms` = milisegundos) | baja |
| `B/op` | memoria reservada por operación | baja |
| `allocs/op` | número de reservas de memoria por operación | baja |
| `B/s`, `msgs/s` | throughput: bytes o mensajes por segundo | sube |

- `± 12%` junto a un valor: cuánto varía entre las 6 rondas. Si es alto, el número es poco fiable.
- En `vs base`: `+25.00% (p=0.002 n=6)` es un cambio **real** (p < 0.05). `~ (p=0.31 n=6)` significa **sin diferencia significativa**: es ruido, ignóralo.
- `geomean` es la media de todos los benchmarks de la tabla: un resumen del cambio global.
- Sin columna `vs base`: el benchmark no existe en `main`, así que no hay con qué comparar.

**Qué hacer.**
- `allocs/op` o `B/op` suben de forma significativa → casi seguro una regresión real (son métricas deterministas). Revísala.
- `sec/op` sube >10% sin `~` → probable regresión; si los `allocs/op` no cambian, puede ser ruido del disco del runner: relanza el job antes de preocuparte.
- `sec/op` cambia <10% o aparece `~` → ignóralo. El fsync en los runners de GitHub es muy variable.

**Qué mide cada benchmark** (todos con fsync real salvo `NoSyncCycle`):

| benchmark | mide |
|---|---|
| `Send/body=N` | enviar mensajes de N bytes con 8×CPU productores concurrentes |
| `SendSequential` | enviar uno a uno, sin concurrencia: coste puro de un envío durable |
| `ReceiveAck` | consumir una cola llena: receive + ack |
| `PersistentCycle` | ciclo completo send + receive + ack |
| `NoSyncCycle` | lo mismo sin fsync: coste de CPU, sin disco |
| `GroupsCycle` | ciclo completo con 64 grupos de orden |
| `Replay` | re-encolar histórico en páginas de 1000 (`msgs/s` = mensajes re-encolados/s) |
| `Open10kLive` | arranque con 10.000 mensajes pendientes |
| `HTTPCycle` | ciclo completo por HTTP + auth + cliente Go (lo más cercano a producción) |

Para pasar `sec/op` a operaciones por segundo: 1 / sec/op. Por ejemplo, 400µs ≈ 2.500 ops/s.

**qbench** (desplegable de abajo): servidor real arrancado en el runner, carga por HTTP. `ops/s` es el throughput conseguido. `p50`/`p90`/`p99` son latencias: el 50/90/99% de las peticiones tardó menos que eso. `end2end` va desde que se envía hasta que se hace ack. La prueba de 50 msg/s muestra la latencia con carga normal: es la que más se parece a tu uso real.

</details>
