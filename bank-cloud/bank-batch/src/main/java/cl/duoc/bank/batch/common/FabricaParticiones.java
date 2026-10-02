package cl.duoc.bank.batch.common;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

/**
 * Arma el Step manager de un Step particionado.
 *
 * Los tres Jobs particionan igual, asi que el cableado vive aca una sola vez y
 * cada JobConfig lo invoca con su propio worker y su propio CSV. Duplicar este
 * bloque tres veces era la alternativa, y con ella cualquier ajuste de
 * particionado habria que hacerlo en tres lugares.
 *
 * Estructura que se construye, la misma de la Figura 1 de la guia:
 *
 *   managerStep (PartitionStep)
 *     |- ParticionadorPorRango  ... divide el CSV en gridSize rangos
 *     |- TaskExecutorPartitionHandler
 *          |- workerStep sobre particion0   \
 *          |- workerStep sobre particion1    >  en paralelo
 *          |- workerStep sobre particion2   /
 *     |- join + aggregate  ... el manager termina cuando terminan todas
 *
 * El manager NO procesa filas: solo reparte, espera y consolida. Si una
 * particion falla, el manager termina FAILED, y en la re-ejecucion Spring Batch
 * vuelve a lanzar unicamente las particiones que no completaron.
 */
@Slf4j
@Component
public class FabricaParticiones {

    private final JobRepository jobRepository;
    private final ThreadPoolTaskExecutor particionTaskExecutor;
    private final EscaladoConfig escalado;
    private final ConteoStepListener conteoListener;

    public FabricaParticiones(JobRepository jobRepository,
                              @Qualifier("particionTaskExecutor") ThreadPoolTaskExecutor particionTaskExecutor,
                              EscaladoConfig escalado,
                              ConteoStepListener conteoListener) {
        this.jobRepository = jobRepository;
        this.particionTaskExecutor = particionTaskExecutor;
        this.escalado = escalado;
        this.conteoListener = conteoListener;
    }

    /**
     * @param nombreManager nombre del Step particionado que ve el Job
     * @param worker        Step de chunk que procesa UNA particion
     * @param recurso       CSV a repartir
     * @return el Step manager, listo para enchufar al flujo del Job
     */
    public Step particionar(String nombreManager, Step worker, Resource recurso) {

        Partitioner particionador = new ParticionadorPorRango(recurso, 1);

        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setStep(worker);
        handler.setTaskExecutor(particionTaskExecutor);
        handler.setGridSize(escalado.getGridSize());
        try {
            handler.afterPropertiesSet();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "No se pudo inicializar el PartitionHandler de " + nombreManager, e);
        }

        log.info("Step particionado '{}': worker '{}', gridSize {}, sobre {}",
                nombreManager, worker.getName(), escalado.getGridSize(), recurso.getFilename());

        return new StepBuilder(nombreManager, jobRepository)
                .partitioner(worker.getName(), particionador)
                .partitionHandler(handler)
                // El manager tambien mide. Su duracion es el tiempo de pared del
                // Step particionado completo -reparto, ejecucion de todas las
                // particiones y consolidacion-, que es la unica cifra
                // comparable de forma honesta contra la del modo multihilo.
                // Las duraciones de los workers por separado no sirven para
                // eso: la del mas rapido subestima y la suma sobreestima.
                .listener((StepExecutionListener) conteoListener)
                .build();
    }
}
