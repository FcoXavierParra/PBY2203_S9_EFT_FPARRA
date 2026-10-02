package cl.duoc.bank.batch.common;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Listener de Job. Es la fuente de los logs de rendimiento de la entrega.
 *
 * Al inicio deja constancia de con que recursos se corrio (CPUs, heap, tamano
 * del pool de hilos); al final, de cuanto costo y que produjo cada Step. Sin
 * ese par de bloques no hay forma de justificar despues por que el pool quedo
 * en 3 hilos o el chunk en 5: la unica base para ajustar esos parametros es
 * medir y comparar corridas.
 *
 * Los tiempos se toman con nanoTime propio en vez de leer
 * JobExecution#getEndTime(): el listener corre dentro del ciclo de vida del Job
 * y no depende de en que momento exacto el framework estampa esa fecha.
 */
@Slf4j
@Component
public class JobCompletionListener implements JobExecutionListener, ExitCodeGenerator {

    private static final long MB = 1024L * 1024L;

    private final ThreadPoolTaskExecutor taskExecutor;

    /** jobExecutionId -> nanoTime de inicio. */
    private final Map<Long, Long> inicios = new ConcurrentHashMap<>();

    /**
     * Codigo de salida del proceso. Lo consulta Spring Boot al cerrar, via
     * SpringApplication.exit() en el main. volatile porque lo escribe el hilo
     * del Job y lo lee el hilo principal.
     */
    private volatile int exitCode = 0;

    @Override
    public int getExitCode() {
        return exitCode;
    }

    public JobCompletionListener(ThreadPoolTaskExecutor batchTaskExecutor) {
        this.taskExecutor = batchTaskExecutor;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        inicios.put(jobExecution.getId(), System.nanoTime());

        Runtime rt = Runtime.getRuntime();

        log.info("#################################################################");
        log.info(" INICIA Job '{}'  (JobExecution id={})",
                jobExecution.getJobInstance().getJobName(), jobExecution.getId());
        log.info(" Parametros           : {}", jobExecution.getJobParameters());
        log.info(" --- recursos disponibles ------------------------------------");
        log.info(" CPUs para la JVM     : {}", rt.availableProcessors());
        log.info(" Heap maximo          : {} MB", rt.maxMemory() / MB);
        log.info(" Heap reservado ahora : {} MB", rt.totalMemory() / MB);
        log.info(" --- configuracion del pool ---------------------------------");
        log.info(" Hilos (core/max)     : {} / {}",
                taskExecutor.getCorePoolSize(), taskExecutor.getMaxPoolSize());
        log.info(" Prefijo de hilo      : {}", taskExecutor.getThreadNamePrefix());
        log.info(" Hilos activos        : {}", taskExecutor.getActiveCount());
        log.info("#################################################################");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        Long inicio = inicios.remove(jobExecution.getId());
        long duracionMs = inicio == null ? -1 : (System.nanoTime() - inicio) / 1_000_000L;

        Runtime rt = Runtime.getRuntime();
        long heapUsado = (rt.totalMemory() - rt.freeMemory()) / MB;

        long leidos = 0;
        long escritos = 0;
        long descartados = 0;

        log.info("#################################################################");
        log.info(" TERMINA Job '{}' -> {} (exitCode={})",
                jobExecution.getJobInstance().getJobName(),
                jobExecution.getStatus(),
                jobExecution.getExitStatus().getExitCode());
        log.info(" Duracion total       : {} ms", duracionMs);
        log.info(" Heap en uso al final : {} MB de {} MB", heapUsado, rt.maxMemory() / MB);
        log.info(" --- detalle por Step ---------------------------------------");

        for (StepExecution se : jobExecution.getStepExecutions()) {
            long saltados = se.getReadSkipCount() + se.getProcessSkipCount() + se.getWriteSkipCount();
            leidos += se.getReadCount();
            escritos += se.getWriteCount();
            descartados += saltados;

            log.info("   {} -> {} | leidas={} escritas={} filtradas={} descartadas={} "
                            + "commits={} rollbacks={}",
                    se.getStepName(), se.getStatus(),
                    se.getReadCount(), se.getWriteCount(), se.getFilterCount(), saltados,
                    se.getCommitCount(), se.getRollbackCount());
        }

        log.info(" --- totales del Job ----------------------------------------");
        log.info(" leidas={} escritas={} descartadas={}", leidos, escritos, descartados);
        if (duracionMs > 0) {
            log.info(" Rendimiento global   : {} filas escritas/segundo",
                    String.format("%.1f", escritos * 1000.0 / duracionMs));
        }

        // EFT: con la reejecucion automatica un mismo proceso puede correr el
        // Job varias veces. El codigo de salida es el de la ULTIMA ejecucion:
        // si el relanzamiento termino bien, el proceso sale con 0.
        exitCode = 0;
        if (jobExecution.getStatus().isUnsuccessful()) {
            exitCode = 1;
            log.error(" El Job NO termino correctamente (el proceso saldra con codigo 1). "
                            + "Excepciones registradas: {}",
                    jobExecution.getAllFailureExceptions().size());
            jobExecution.getAllFailureExceptions()
                    .forEach(t -> log.error("   causa: {}", t.toString()));
        }

        log.info(" Resumen JobExecution : {}", jobExecution);
        log.info("#################################################################");
    }
}
