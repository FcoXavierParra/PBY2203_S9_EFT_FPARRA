package cl.duoc.bank.batch.common;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Escribe cada registro que un Step descarto, con el motivo, en un CSV.
 *
 * Existe porque el log de consola se pierde: cuando el Job termina COMPLETED
 * habiendo saltado filas, alguien tiene que poder abrir un archivo despues y
 * ver exactamente que se dejo fuera. En una migracion bancaria eso no es
 * opcional.
 *
 * UN ARCHIVO POR PARTICION
 * ------------------------
 * En la semana 2 esto era un unico archivo con un unico writer, porque habia un
 * unico Step. Con particiones esa forma se rompe de una manera silenciosa y
 * cara: cada particion es una StepExecution propia, asi que beforeStep y
 * afterStep se disparan N veces. Con un solo writer, la primera particion en
 * terminar cerraba el archivo y las demas seguian descartando filas contra un
 * writer nulo. El Job terminaba COMPLETED y el archivo de auditoria salia
 * incompleto sin un solo mensaje de error: exactamente el fallo que este
 * componente existe para evitar.
 *
 * La solucion es un writer por StepExecution, en un mapa. Es ademas lo que
 * describe la guia de la semana: como el control de errores se maneja en cada
 * particion, la salida del proceso crea un archivo de errores por particion.
 *
 * COMO SE RESUELVE A QUE ARCHIVO ESCRIBIR
 * ---------------------------------------
 * El SkipListener no recibe el contexto del Step, asi que hay que deducirlo:
 *
 *  1. Si el hilo actual abrio una salida, se usa esa. Es el caso particionado:
 *     una particion se ejecuta completa en un mismo hilo, de modo que el
 *     ThreadLocal fijado en beforeStep sigue vigente cuando ocurre el skip.
 *  2. Si no, y hay exactamente una salida abierta, se usa esa. Es el caso
 *     multihilo: beforeStep corre en el hilo principal y los chunks en
 *     Batch-Thread-N, que no heredan el ThreadLocal, pero solo hay un archivo
 *     posible y no existe ambiguedad.
 *
 * Si no aplica ninguno de los dos, se descarta la escritura y queda el log. No
 * se adivina: escribir en el archivo equivocado seria peor que no escribir.
 */
@Slf4j
@Component
public class RegistroErrores {

    private static final String CABECERA = "timestamp,hilo,step,particion,etapa,motivo,contenido";

    private final Path archivoBase;

    private final Map<String, Salida> salidas = new ConcurrentHashMap<>();
    private final ThreadLocal<String> claveDelHilo = new ThreadLocal<>();
    private final AtomicInteger totalEscritos = new AtomicInteger();

    public RegistroErrores(@Value("${bank.errores.archivo:reportes/errores.csv}") String ruta) {
        this.archivoBase = Paths.get(ruta);
    }

    /** Estado de un archivo de errores: su writer, su ruta y su contador. */
    private static final class Salida {
        private final Path ruta;
        private final BufferedWriter writer;
        private final String step;
        private final String particion;
        private final AtomicInteger escritos = new AtomicInteger();

        private Salida(Path ruta, BufferedWriter writer, String step, String particion) {
            this.ruta = ruta;
            this.writer = writer;
            this.step = step;
            this.particion = particion;
        }
    }

    /**
     * Abre el archivo de esta StepExecution y escribe la cabecera.
     *
     * El nombre lo da Spring Batch. En un Step normal es
     * leerTransaccionesStep; en una particion viene con el sufijo
     * leerTransaccionesWorkerStep:particion1.
     */
    public void abrir(String nombreStepExecution) {
        claveDelHilo.set(nombreStepExecution);

        salidas.computeIfAbsent(nombreStepExecution, clave -> {
            String particion = particionDe(clave);
            Path ruta = rutaPara(particion);
            try {
                if (ruta.getParent() != null) {
                    Files.createDirectories(ruta.getParent());
                }
                BufferedWriter writer = Files.newBufferedWriter(ruta, StandardCharsets.UTF_8);
                writer.write(CABECERA);
                writer.newLine();
                log.info("Archivo de errores abierto: {}", ruta.toAbsolutePath());
                return new Salida(ruta, writer, stepDe(clave), particion);
            } catch (IOException e) {
                // No se propaga: que falle el archivo de auditoria no debe botar
                // el Job. Queda el log como respaldo.
                log.error("No se pudo abrir el archivo de errores {}: {}", ruta, e.getMessage());
                return null;
            }
        });
    }

    /**
     * Registra una fila descartada.
     *
     * @param etapa     LECTURA, PROCESO o ESCRITURA
     * @param motivo    excepcion y mensaje que causaron el descarte
     * @param contenido la fila o el objeto que se descarto
     */
    public void registrar(String etapa, String motivo, String contenido) {
        Salida salida = resolver();
        if (salida == null) {
            log.warn("Fila descartada sin archivo de errores disponible ({}): {}", etapa, motivo);
            return;
        }
        // El candado es por archivo, no global: dos particiones escribiendo a
        // la vez en archivos distintos no se estorban. BufferedWriter no es
        // thread-safe, y en modo multihilo si hay varios hilos sobre el mismo.
        synchronized (salida) {
            try {
                salida.writer.write(String.join(",",
                        csv(LocalDateTime.now().toString()),
                        csv(Thread.currentThread().getName()),
                        csv(salida.step),
                        csv(salida.particion == null ? "" : salida.particion),
                        csv(etapa),
                        csv(motivo),
                        csv(contenido)));
                salida.writer.newLine();
                salida.escritos.incrementAndGet();
                totalEscritos.incrementAndGet();
            } catch (IOException e) {
                log.error("No se pudo registrar el error en {}: {}", salida.ruta, e.getMessage());
            }
        }
    }

    /** Cierra el archivo de esta StepExecution y devuelve cuantas filas registro. */
    public int cerrar(String nombreStepExecution) {
        Salida salida = salidas.remove(nombreStepExecution);
        claveDelHilo.remove();

        if (salida == null) {
            return 0;
        }
        int total = salida.escritos.get();
        synchronized (salida) {
            try {
                salida.writer.flush();
                salida.writer.close();
                log.info("Archivo de errores cerrado: {} fila(s) registrada(s) en {}",
                        total, salida.ruta.toAbsolutePath());
            } catch (IOException e) {
                log.error("No se pudo cerrar el archivo de errores {}: {}", salida.ruta, e.getMessage());
            }
        }
        return total;
    }

    /** Total acumulado en la corrida, sumando todas las particiones. */
    public int getEscritos() {
        return totalEscritos.get();
    }

    // ------------------------------------------------------------- INTERNOS

    private Salida resolver() {
        String clave = claveDelHilo.get();
        if (clave != null) {
            Salida propia = salidas.get(clave);
            if (propia != null) {
                return propia;
            }
        }
        if (salidas.size() == 1) {
            return salidas.values().iterator().next();
        }
        return null;
    }

    /** Devuelve la parte posterior a los dos puntos, o null si no la hay. */
    private static String particionDe(String nombreStepExecution) {
        int corte = nombreStepExecution.indexOf(':');
        return corte < 0 ? null : nombreStepExecution.substring(corte + 1);
    }

    /** Devuelve la parte anterior a los dos puntos, o el nombre completo. */
    private static String stepDe(String nombreStepExecution) {
        int corte = nombreStepExecution.indexOf(':');
        return corte < 0 ? nombreStepExecution : nombreStepExecution.substring(0, corte);
    }

    /** De reportes/errores.csv produce reportes/errores_particion1.csv */
    private Path rutaPara(String particion) {
        if (particion == null) {
            return archivoBase;
        }
        String nombre = archivoBase.getFileName().toString();
        int punto = nombre.lastIndexOf('.');
        String base = punto < 0 ? nombre : nombre.substring(0, punto);
        String extension = punto < 0 ? "" : nombre.substring(punto);
        Path padre = archivoBase.getParent();
        String nuevo = base + "_" + particion + extension;
        return padre == null ? Paths.get(nuevo) : padre.resolve(nuevo);
    }

    /**
     * Escapa un campo para CSV. Los motivos de error traen comas y comillas
     * (por ejemplo el contenido de la linea que no se pudo parsear), asi que
     * sin escapar el archivo quedaria con columnas corridas.
     */
    private static String csv(String valor) {
        String v = valor == null ? "" : valor.replace("\r", " ").replace("\n", " ");
        return "\"" + v.replace("\"", "\"\"") + "\"";
    }
}
