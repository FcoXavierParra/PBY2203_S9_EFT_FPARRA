package cl.duoc.bank.batch.common;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Pool de hilos que ejecuta los chunks de los Steps en paralelo.
 *
 * El enunciado de la semana 2 pide "3 hilos de ejecucion paralela con chunks de
 * tamano 5". Por eso corePoolSize = maxPoolSize = 3: el pool queda FIJO en tres
 * hilos, ni crece ni se encoge.
 *
 * Se aparta a proposito del ejemplo de la guia (core 5 / max 10). Un pool que
 * puede crecer a 10 mientras el trabajo real se limita a 3 tareas simultaneas
 * deja hilos creados y ociosos: es memoria de stack reservada que nunca se usa,
 * justo lo contrario de "optimizar recursos del sistema". Con core = max = 3 no
 * hay creacion ni destruccion de hilos durante la corrida.
 *
 * queueCapacity = 25 no cambia el paralelismo (siguen siendo 3 hilos), solo
 * evita que un chunk se rechace con RejectedExecutionException cuando los tres
 * hilos estan ocupados: espera en la cola hasta que uno se libere.
 *
 * El prefijo "Batch-Thread-" es lo que hace verificable el paralelismo: en el
 * log cada linea sale con [Batch-Thread-1..3], y es la evidencia que se entrega.
 */
@Slf4j
@Configuration
public class BatchTaskExecutorConfig {

    @Bean(name = "batchTaskExecutor", destroyMethod = "shutdown")
    public ThreadPoolTaskExecutor batchTaskExecutor(
            @Value("${bank.batch.hilos:3}") int hilos,
            @Value("${bank.batch.cola:25}") int cola) {

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(hilos);
        executor.setMaxPoolSize(hilos);
        executor.setQueueCapacity(cola);
        executor.setThreadNamePrefix("Batch-Thread-");
        // Espera a que los chunks en vuelo terminen antes de cerrar el pool.
        // Sin esto, un shutdown del contexto podria cortar un chunk a medias.
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        //
        // setDaemon(true) NO es opcional en una aplicacion batch de linea de
        // comandos, y equivocarse aqui cuelga el proceso.
        //
        // Esta app no es un servidor: JobLauncherApplicationRunner corre el Job
        // durante el arranque, SpringApplication.run() retorna y main() termina.
        // Recien ahi la JVM corre los shutdown hooks... pero solo cuando ya no
        // queda ningun hilo non-daemon vivo. Los hilos 'core' de un
        // ThreadPoolTaskExecutor son non-daemon por defecto y no mueren nunca:
        // el Job termina COMPLETED, se imprime el resumen, y el proceso se queda
        // colgado para siempre esperando a tres hilos que esperan trabajo que ya
        // no va a llegar. El hook que cerraria el contexto (y con el el pool)
        // nunca alcanza a ejecutarse.
        //
        // Con daemon = true la JVM puede terminar y el destroyMethod = "shutdown"
        // del bean cierra el pool ordenadamente. No hay riesgo de cortar un
        // chunk a medias: cuando main() retorna, el Job ya termino.
        executor.setDaemon(true);
        executor.initialize();

        log.info("TaskExecutor batch inicializado: {} hilos fijos, cola de {}, prefijo '{}'",
                hilos, cola, "Batch-Thread-");
        log.info("CPUs disponibles para la JVM: {}", Runtime.getRuntime().availableProcessors());

        return executor;
    }

    /**
     * Pool que ejecuta las PARTICIONES. Es un pool distinto del anterior a
     * proposito, y la razon no es cosmetica.
     *
     * En modo particionado cada particion es una StepExecution completa que
     * ocupa su hilo de principio a fin: lee, procesa, escribe y hace commit sin
     * soltarlo. Si compartiera el pool con los chunks del modo multihilo, con
     * gridSize mayor que el tamano del pool las particiones se quedarian
     * esperando un hilo que no se libera hasta que otra particion TERMINE, y el
     * paralelismo se degradaria a ejecucion por tandas sin que el log lo diga.
     *
     * Se dimensiona con bank.escalado.hilos-particion, independiente de
     * bank.batch.hilos, para poder barrer las dos variables por separado en el
     * benchmark: 4 particiones sobre 2 hilos y 4 sobre 4 miden cosas distintas.
     *
     * El prefijo 'Particion-' hace que el log distinga de un vistazo en que
     * modo corrio: [Batch-Thread-N] es multihilo, [Particion-N] es particionado.
     */
    @Bean(name = "particionTaskExecutor", destroyMethod = "shutdown")
    public ThreadPoolTaskExecutor particionTaskExecutor(
            @Value("${bank.escalado.hilos-particion:3}") int hilos) {

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(hilos);
        executor.setMaxPoolSize(hilos);
        // Cola holgada: con gridSize alto las particiones que no alcanzan hilo
        // esperan aca en vez de ser rechazadas.
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("Particion-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        // Mismo motivo que en batchTaskExecutor: sin daemon el proceso de linea
        // de comandos no termina nunca.
        executor.setDaemon(true);
        executor.initialize();

        log.info("TaskExecutor de particiones inicializado: {} hilos fijos, prefijo 'Particion-'", hilos);

        return executor;
    }
}
