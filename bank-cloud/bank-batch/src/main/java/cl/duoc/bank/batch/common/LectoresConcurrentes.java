package cl.duoc.bank.batch.common;

import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;

/**
 * Envuelve un ItemReader para que se pueda usar desde un Step multihilo.
 *
 * Es el detalle mas facil de pasar por alto al paralelizar y el mas caro:
 * FlatFileItemReader NO es thread-safe. Mantiene la posicion del archivo en un
 * campo (el numero de linea actual), y read() la lee e incrementa sin candado.
 * Con tres hilos llamando read() a la vez, dos pueden recibir la misma linea o
 * saltarse una. El sintoma es peor que una excepcion: el Job termina COMPLETED
 * con filas duplicadas o perdidas en la base, y nadie se entera.
 *
 * SynchronizedItemStreamReader pone un candado alrededor de read(), de modo que
 * la lectura queda serializada mientras el procesamiento y la escritura -que es
 * donde esta el costo real- siguen ocurriendo en paralelo.
 *
 * Nota sobre saveState: los lectores que se envuelven aqui se construyen con
 * saveState(false) a proposito. Con varios hilos en vuelo, la posicion que se
 * guardaria en el ExecutionContext no corresponde a un punto consistente del
 * archivo, y un reinicio a medias retomaria en el lugar equivocado. Es la
 * recomendacion de la documentacion de Spring Batch para steps multihilo. La
 * contrapartida asumida: un Step que falla se reintenta desde el comienzo del
 * archivo, no desde donde quedo. Es seguro porque los writers son idempotentes
 * (merge por clave en Jobs 1 y 2, y truncado previo en el Job 3).
 */
public final class LectoresConcurrentes {

    private LectoresConcurrentes() {
        // Clase de utilidad.
    }

    public static <T> SynchronizedItemStreamReader<T> sincronizar(ItemStreamReader<T> delegado) {
        SynchronizedItemStreamReader<T> lector = new SynchronizedItemStreamReader<>();
        lector.setDelegate(delegado);
        return lector;
    }
}
