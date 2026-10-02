package cl.duoc.bank.batch.common;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.Resource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Divide un CSV en rangos contiguos de filas, uno por particion.
 *
 * Es el StepExecutionSplitter del que habla la guia: cada ExecutionContext que
 * se devuelve aqui termina siendo una StepExecution independiente, y el
 * TaskExecutorPartitionHandler las lanza en paralelo.
 *
 * Reparto por RANGO DE FILAS y no por archivo
 * -------------------------------------------
 * La alternativa obvia seria una particion por archivo (transacciones,
 * intereses, cuentas). No sirve para este caso por dos razones: los tres
 * archivos alimentan Jobs distintos, y sus tamanos son muy desparejos, asi que
 * el Job entero duraria lo que el archivo mas grande. Repartir por rango de
 * filas dentro de UN archivo da particiones de tamano casi identico, que es la
 * condicion para que el paralelismo rinda: si una particion es el doble que las
 * otras, el Step dura lo que esa, y los demas hilos esperan.
 *
 * Las claves 'inicio' y 'fin' viajan al lector como currentItemCount y
 * maxItemCount. Son indices de ITEM, no de linea del archivo: la cabecera ya se
 * descuenta con linesToSkip, asi que el item 0 es la primera fila de datos.
 *
 * El conteo previo abre el archivo una vez de mas. Es barato comparado con el
 * proceso completo (una pasada secuencial sin parseo ni BD) y es lo que permite
 * que las particiones queden parejas sin que nadie codifique el total a mano.
 */
@Slf4j
public class ParticionadorPorRango implements Partitioner {

    public static final String CLAVE_INICIO = "inicio";
    public static final String CLAVE_FIN = "fin";
    public static final String CLAVE_NOMBRE = "nombreParticion";

    private final Resource recurso;
    private final int lineasCabecera;

    public ParticionadorPorRango(Resource recurso, int lineasCabecera) {
        this.recurso = recurso;
        this.lineasCabecera = lineasCabecera;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {

        int total = contarFilasDeDatos();
        Map<String, ExecutionContext> particiones = new LinkedHashMap<>();

        if (total == 0) {
            log.warn("El recurso {} no tiene filas de datos: se crea una particion vacia",
                    recurso.getFilename());
            ExecutionContext vacia = new ExecutionContext();
            vacia.putInt(CLAVE_INICIO, 0);
            vacia.putInt(CLAVE_FIN, 0);
            vacia.putString(CLAVE_NOMBRE, "particion0");
            particiones.put("particion0", vacia);
            return particiones;
        }

        // Nunca mas particiones que filas: una particion vacia solo agrega
        // el costo de arrancar una StepExecution que no lee nada.
        int grid = Math.min(gridSize, total);

        // El reparto de la division inexacta se hace de a una fila entre las
        // primeras particiones, en vez de acumular el resto en la ultima. Con
        // 1000 filas y grid 3 quedan 334/333/333 y no 333/333/334; la
        // diferencia es irrelevante aqui, pero con restos grandes evita que la
        // ultima particion sea la que retrasa a todas.
        int base = total / grid;
        int resto = total % grid;

        int inicio = 0;
        for (int i = 0; i < grid; i++) {
            int tamano = base + (i < resto ? 1 : 0);
            int fin = inicio + tamano;

            String nombre = "particion" + i;
            ExecutionContext contexto = new ExecutionContext();
            contexto.putInt(CLAVE_INICIO, inicio);
            contexto.putInt(CLAVE_FIN, fin);
            contexto.putString(CLAVE_NOMBRE, nombre);
            particiones.put(nombre, contexto);

            log.info("Particion {} -> filas [{}, {}) = {} filas", nombre, inicio, fin, tamano);
            inicio = fin;
        }

        log.info("Particionado de {}: {} filas de datos repartidas en {} particion(es)",
                recurso.getFilename(), total, grid);

        return particiones;
    }

    /**
     * Cuenta las filas de datos del CSV descontando la cabecera.
     *
     * Se ignoran las lineas en blanco del final, que son habituales en archivos
     * exportados desde sistemas legacy y que el lector tampoco entrega como
     * item. Si se contaran, la ultima particion pediria filas que no existen y
     * terminaria antes de tiempo sin que nadie lo note.
     */
    private int contarFilasDeDatos() {
        try (BufferedReader lector = new BufferedReader(
                new InputStreamReader(recurso.getInputStream(), StandardCharsets.UTF_8))) {

            int lineas = 0;
            String linea;
            while ((linea = lector.readLine()) != null) {
                if (!linea.isBlank()) {
                    lineas++;
                }
            }
            return Math.max(0, lineas - lineasCabecera);

        } catch (IOException e) {
            throw new IllegalStateException(
                    "No se pudo contar las filas de " + recurso.getFilename()
                            + " para particionar: " + e.getMessage(), e);
        }
    }
}
