# Instrucciones para ejecutar y probar cada componente

Banco XYZ · PBY2203 Evaluación Final Transversal · Francisco Javier Parra Andía

Este documento explica cómo levantar y verificar cada parte del sistema **en un equipo local**.
El despliegue en la nube (AWS) está en [`despliegue.md`](despliegue.md).

## Requisitos

| Para | Necesitas |
|---|---|
| Procesos batch (parte 1) | **JDK 21** y **Maven 3.9+** |
| BFF y microservicios (partes 2 y 3) | **Docker** con Compose v2 y unos **6 GB de RAM** libres. No hace falta JDK: se compila dentro de Docker |
| Probar los endpoints | `curl` (en Windows, `curl.exe` de System32) |

```bash
git clone https://github.com/FcoXavierParra/PBY2203_S9_EFT_FPARRA.git
cd PBY2203_S9_EFT_FPARRA/bank-cloud
```

---

## 1. Procesos batch (Spring Batch)

### Compilar

```bash
mvn clean package -DskipTests -pl bank-batch -am
```

### Ejecutar cada Job

Cada ejecución corre **un** Job y termina. Por defecto usa una base H2 en memoria y el dataset
oficial `fin_legacy_data/semana_3`, sin parámetros adicionales.

```bash
java -jar bank-batch/target/bank-batch-0.0.1-SNAPSHOT.jar --spring.batch.job.name=transaccionesJob
java -jar bank-batch/target/bank-batch-0.0.1-SNAPSHOT.jar --spring.batch.job.name=interesesJob
java -jar bank-batch/target/bank-batch-0.0.1-SNAPSHOT.jar --spring.batch.job.name=anualesJob
```

| Job | Proceso legacy que reemplaza | Lee | Produce |
|---|---|---|---|
| `transaccionesJob` | Reporte de Transacciones Diarias | `movimientos_financieros_diarios.csv` | transacciones válidas, anomalías marcadas y resumen por día (`resumen_diario`) |
| `interesesJob` | Cálculo de Intereses | `intereses_trimestrales.csv` | interés de cada cuenta de ahorro o préstamo |
| `anualesJob` | Estados de Cuenta Anuales | `estados_financieros_anuales.csv` | estado de cuenta por cuenta, más el informe `reportes/estados_cuenta_anuales.txt` |

**Qué verificar:** al final de cada corrida el log muestra `TERMINA Job '...' -> COMPLETED`, las
filas leídas, escritas y descartadas por step, y el proceso sale con **código 0**. Las filas
inválidas (montos negativos, fechas mal formateadas, tipos desconocidos, duplicados) no detienen
el Job: se omiten y quedan registradas con su motivo en `reportes/errores.csv`.

Resultado esperado con el dataset oficial (ver `evidencias/01` a `03`):

| Job | Descartadas | Estado |
|---|---|---|
| transacciones | 520 de 1000 (52 %) | COMPLETED |
| intereses | 681 de 1000 (68 %) | COMPLETED |
| anuales | 150 de 1000 (15 %) | COMPLETED |

### Probar la reejecución automática ante un fallo crítico

Simula que la base no responde en las dos primeras verificaciones de conexión:

```bash
java -jar bank-batch/target/bank-batch-0.0.1-SNAPSHOT.jar --spring.batch.job.name=interesesJob \
     --bank.simulacion.fallas-conexion=2
```

**Qué verificar** (`evidencias/04_batch_reejecucion_automatica.txt`): el Job termina FAILED,
se relanza solo a los 5 s, falla de nuevo, se relanza a los 10 s y termina **COMPLETED**, sobre
la **misma JobInstance**. El proceso sale con código 0.

### Probar la política de finalización por calidad de datos

```bash
java -jar bank-batch/target/bank-batch-0.0.1-SNAPSHOT.jar --spring.batch.job.name=transaccionesJob \
     --bank.politicas.tolerancia-descartes=0.10
```

**Qué verificar** (`evidencias/05_batch_rechazo_por_calidad.txt`): con una tolerancia de 10 %,
el 52 % de descartes hace que el Job termine FAILED **sin relanzarse**: un archivo que viene
mal no se arregla reintentando. El proceso sale con código 1.

### Paralelismo

```bash
# varios hilos sobre un mismo step
java -jar bank-batch/target/bank-batch-0.0.1-SNAPSHOT.jar --spring.batch.job.name=anualesJob \
     --bank.escalado.modo=multihilo --bank.batch.hilos=3
# el archivo dividido en particiones, cada una en su hilo
java -jar bank-batch/target/bank-batch-0.0.1-SNAPSHOT.jar --spring.batch.job.name=anualesJob \
     --bank.escalado.modo=particiones --bank.escalado.grid-size=3
```

### Contra Oracle

```bash
export ORACLE_JDBC_URL="jdbc:oracle:thin:@<servicio>_low?TNS_ADMIN=/ruta/al/wallet"
export ORACLE_USER=<usuario>  ORACLE_PASSWORD=<clave>
java -jar bank-batch/target/bank-batch-0.0.1-SNAPSHOT.jar --spring.profiles.active=oracle \
     --spring.batch.job.name=transaccionesJob
```

---

## 2 y 3. BFF y microservicios

### Levantar el ecosistema (local, con H2)

```bash
cp .env.example .env
# en el .env:  PERFIL_BD=default   CUENTAS_REPLICAS=1
#              TRANSF_TIMEOUT_LECTURA_MS=2500   TRANSF_LLAMADA_LENTA=2s
docker compose up -d --build     # la primera vez descarga dependencias: varios minutos
docker compose ps                # esperar a que los servicios digan (healthy)
```

En modo H2 cada contenedor tiene su propia base en memoria, cargada desde los CSV oficiales;
por eso `ms-cuentas` va con **una** réplica. Con Oracle (ver `despliegue.md`) corre con dos.

| Componente | Puerto | Cómo comprobar que está arriba |
|---|---|---|
| config-server | 7888 (interno) | `docker compose ps` → healthy |
| eureka-server | 8761 | `http://localhost:8761`: lista las instancias registradas |
| auth-server | 9000 | `curl http://localhost:9000/.well-known/oauth-authorization-server` |
| broker-mensajeria | 61616 / 8161 | `curl -u "admin-broker:$BROKER_CLAVE_ADMIN" http://localhost:8161/admin/topologia` |
| ms-cuentas, ms-transferencias, ms-auditoria | 8090-8092 (internos) | healthy, y registrados en Eureka |
| bff-web / bff-movil / bff-cajero | 8081 / 8082 / 8083 | `curl -k https://localhost:8081/actuator/health` |

### Usuarios de prueba

Las claves no se escriben aquí. Cada una se lee de una variable de entorno y, si no está
definida, se usa el valor de desarrollo de `config-repo/auth-server.yml` (personas y clientes
OAuth) o `config-repo/broker-mensajeria.yml` (usuarios del broker). Un despliegue real las
define en su entorno.

| Usuario | Variable de la clave | Cuenta | Canales |
|---|---|---|---|
| `cliente` | `AUTH_CLAVE_CLIENTE` | 105 | web, móvil |
| `cliente2` | `AUTH_CLAVE_CLIENTE2` | 107 | web, móvil |
| `ejecutivo` | `AUTH_CLAVE_EJECUTIVO` | cartera completa | web |
| terminal `cajero-terminal-01` | `AUTH_SECRETO_TERMINAL_01` | — | cajero (PIN `1234`) |
| `admin-broker` (consola del broker) | `BROKER_CLAVE_ADMIN` | — | — |

Los ejemplos con `curl` de más abajo toman las dos últimas de la terminal:

```bash
export AUTH_SECRETO_TERMINAL_01=<secreto del terminal>
export BROKER_CLAVE_ADMIN=<clave de admin-broker>
```

### Probar OAuth 2.0 y los tres canales

La prueba completa es `evidencia_nube.ps1`, que ejecuta los flujos paso a paso
(`authorization_code` con PKCE para personas y `client_credentials` para máquinas) y prueba
cada canal, incluidos los rechazos y las fallas reales. Está pensado para la EC2 (ver
`despliegue.md`); el flujo de personas, paso a paso, está en `herramientas/oauth_funciones.ps1`.

Pruebas puntuales con `curl`:

```bash
# 1. Un token de máquina (client_credentials) para el terminal del cajero
curl -s -u "cajero-terminal-01:$AUTH_SECRETO_TERMINAL_01" \
     -d grant_type=client_credentials -d scope=cajero.terminal \
     http://localhost:9000/oauth2/token
# 2. Abrir sesión en el cajero con ese token y el PIN del cliente
curl -k -X POST https://localhost:8083/api/cajero/sesion \
     -H "Authorization: Bearer <token>" -H "Content-Type: application/json" \
     -d '{"cuentaId":105,"pin":"1234"}'
# 3. Consultar saldo con la sesión
curl -k https://localhost:8083/api/cajero/saldo \
     -H "Authorization: Bearer <token>" -H "X-Sesion-Cajero: <sesion>"
# 4. Sin token: 401
curl -k -o /dev/null -w "%{http_code}\n" https://localhost:8081/api/web/cuentas/105
```

| Prueba | Resultado esperado |
|---|---|
| Cualquier BFF sin token | 401 |
| Cliente pidiendo una cuenta ajena | 403 |
| Token del canal web en el BFF móvil | 401 (cada canal exige su propio `aud`) |
| Transferencia web | 202 PENDIENTE; segundos después el saldo cambió en los tres canales |
| Respuesta de la misma cuenta | web: ficha completa · móvil: resumen liviano · cajero: solo saldo |

### Probar la resiliencia

```bash
docker compose stop ms-transferencias    # transferir -> 503 con mensaje del canal; el circuito abre
docker compose start ms-transferencias   # el circuito pasa por HALF_OPEN y cierra solo
curl -k https://localhost:8081/actuator/health   # estado de cada Circuit Breaker
```

### Probar la mensajería

```bash
curl -u "admin-broker:$BROKER_CLAVE_ADMIN" http://localhost:8161/admin/topologia
```

Muestra cada tópico de la saga, sus suscripciones, consumidores y mensajes recibidos y
confirmados. Después de una transferencia, los contadores de `solicitada`, `aplicada` y
`cerrada` suben en uno y no quedan mensajes pendientes.

### Detener

```bash
docker compose down
```
