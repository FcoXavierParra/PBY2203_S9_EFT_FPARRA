package cl.duoc.bank.batch.common;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Imprime el resumen de conteos y de rendimiento al terminar cada Step.
 *
 * En la semana 1 este listener solo contaba filas; ahora tambien mide. Los dos
 * numeros que importan para ajustar la configuracion son la duracion y el
 * throughput: son los que permiten comparar una corrida de 1 hilo contra una de
 * 3, o chunk 5 contra chunk 50, y decidir con datos en vez de por intuicion.
 *
 * Implementa ademas ChunkListener para llevar la cuenta de los hilos que
 * efectivamente procesaron chunks. beforeChunk() corre en el hilo del chunk, o
 * sea en 'Batch-Thread-N', asi que basta leer ahi el nombre del hilo actual.
 * Ese conjunto es la prueba directa del paralelismo: si sale
 * {Batch-Thread-1, Batch-Thread-2, Batch-Thread-3} el pool se usa completo; si
 * sale uno solo, el dataset no alcanzo para llenarlo.
 *
 * Los tiempos y los hilos se guardan en mapas por stepExecutionId, no en
 * campos: el bean es singleton y lo comparten los tres Jobs.
 */
@Slf4j
@Component
public class ConteoStepListener implements StepExecutionListener, ChunkListener {

    /** stepExecutionId -> nanoTime de inicio. */
    private final Map<Long, Long> inicios = new ConcurrentHashMap<>();

    /** stepExecutionId -> nombres de los hilos que ejecutaron chunks. */
    private final Map<Long, Set<String>> hilos = new ConcurrentHashMap<>();

    // ------------------------------------------------- StepExecutionListener

    @Override
    public void beforeStep(StepExecution stepExecution) {
        inicios.put(stepExecution.getId(), System.nanoTime());
        hilos.put(stepExecution.getId(), ConcurrentHashMap.newKeySet());

        log.info(">>> Inicia Step '{}' (job '{}')",
                stepExecution.getStepName(),
                stepExecution.getJobExecution().getJobInstance().getJobName());
    }

    @Override
    public ExitStatus afterStep(StepExecution se) {
        Long inicio = inicios.remove(se.getId());
        Set<String> hilosUsados = hilos.remove(se.getId());

        long duracionMs = inicio == null ? -1 : (System.nanoTime() - inicio) / 1_000_000L;
        long saltados = se.getReadSkipCount() + se.getProcessSkipCount() + se.getWriteSkipCount();
        long totalFilas = se.getReadCount() + se.getReadSkipCount();

        log.info("=================================================================");
        log.info(" RESUMEN Step '{}' -> {}", se.getStepName(), se.getStatus());
        log.info("   leidos               : {}", se.getReadCount());
        log.info("   filtrados (negocio)  : {}", se.getFilterCount());
        log.info("   escritos             : {}", se.getWriteCount());
        log.info("   saltados (total)     : {}", saltados);
        log.info("     - en lectura       : {}", se.getReadSkipCount());
        log.info("     - en procesamiento : {}", se.getProcessSkipCount());
        log.info("     - en escritura     : {}", se.getWriteSkipCount());
        log.info("   commits / rollbacks  : {} / {}", se.getCommitCount(), se.getRollbackCount());
        log.info("   --- rendimiento ---------------------------------------------");
        log.info("   duracion             : {} ms", duracionMs);

        if (duracionMs > 0) {
            log.info("   throughput lectura   : {} filas/s",
                    String.format("%.1f", totalFilas * 1000.0 / duracionMs));
            log.info("   throughput escritura : {} filas/s",
                    String.format("%.1f", se.getWriteCount() * 1000.0 / duracionMs));
        }
        if (se.getCommitCount() > 0) {
            log.info("   filas por commit     : {}",
                    String.format("%.1f", (double) se.getWriteCount() / se.getCommitCount()));
        }
        if (hilosUsados == null || hilosUsados.isEmpty()) {
            log.info("   hilos utilizados     : sin registro");
        } else {
            log.info("   hilos utilizados     : {} -> {}",
                    hilosUsados.size(), new TreeSet<>(hilosUsados));
        }
        log.info("=================================================================");

        return se.getExitStatus();
    }

    // ------------------------------------------------------- ChunkListener

    @Override
    public void beforeChunk(ChunkContext context) {
        // Corre en el hilo que va a procesar el chunk.
        Set<String> vistos = hilos.get(context.getStepContext().getStepExecution().getId());
        if (vistos != null) {
            vistos.add(Thread.currentThread().getName());
        }
    }

    @Override
    public void afterChunk(ChunkContext context) {
        // Sin accion: el resumen se emite una sola vez, en afterStep.
    }

    @Override
    public void afterChunkError(ChunkContext context) {
        log.warn("[CHUNK-ERROR] hilo={} step='{}' | el chunk se revierte y Spring Batch lo "
                        + "reprocesa fila por fila para aislar la defectuosa",
                Thread.currentThread().getName(),
                context.getStepContext().getStepName());
    }
}
