package cl.duoc.bank.batch.anuales;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

/**
 * Primer Step del Job 3: deja las tablas anuales vacias antes de recargarlas.
 *
 * Hace falta porque movimiento_anual no tiene llave natural (el CSV no trae
 * id), asi que el writer siempre inserta. Sin esta limpieza, cada re-ejecucion
 * duplicaria los movimientos y el estado de cuenta saldria al doble.
 *
 * Es coherente con el proceso de negocio: un estado de cuenta anual se
 * regenera completo, no se va parchando.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LimpiarAnualesTasklet implements Tasklet {

    private final MovimientoAnualRepository movimientoRepository;
    private final EstadoCuentaAnualRepository estadoRepository;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        long movimientos = movimientoRepository.count();
        long estados = estadoRepository.count();

        // Primero los estados: son el agregado, dependen de los movimientos.
        estadoRepository.deleteAllInBatch();
        movimientoRepository.deleteAllInBatch();

        log.info("Limpieza previa: se eliminaron {} movimientos y {} estados de cuenta de la corrida anterior",
                movimientos, estados);

        return RepeatStatus.FINISHED;
    }
}
