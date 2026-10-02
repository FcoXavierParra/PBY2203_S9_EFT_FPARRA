package cl.duoc.bank.batch.transacciones;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Segundo Step del Job 1: agrega por fecha las transacciones que el primer
 * Step dejo en la base y escribe la tabla resumen_diario.
 *
 * Va como Tasklet y no como chunk porque la unidad de trabajo no es "una
 * fila": hay que ver el conjunto completo para poder agrupar.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ResumenDiarioTasklet implements Tasklet {

    private final TransaccionRepository transaccionRepository;
    private final ResumenDiarioRepository resumenDiarioRepository;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {

        List<Transaccion> transacciones = transaccionRepository.findAll();
        log.info("Generando resumen diario sobre {} transacciones persistidas", transacciones.size());

        // TreeMap para que el log salga ordenado por fecha.
        Map<LocalDate, ResumenDiario> porFecha = new TreeMap<>();

        for (Transaccion t : transacciones) {
            ResumenDiario r = porFecha.computeIfAbsent(t.getFecha(), fecha -> {
                // Si ya existe la fila del dia (re-ejecucion) se reutiliza para
                // actualizarla, en vez de insertar un duplicado.
                ResumenDiario existente = resumenDiarioRepository.findByFecha(fecha)
                        .orElseGet(ResumenDiario::new);
                existente.setFecha(fecha);
                existente.setTotalDebitos(BigDecimal.ZERO);
                existente.setTotalCreditos(BigDecimal.ZERO);
                existente.setCantidadTransacciones(0);
                existente.setCantidadAnomalias(0);
                return existente;
            });

            if ("debito".equals(t.getTipo())) {
                r.setTotalDebitos(r.getTotalDebitos().add(t.getMonto()));
            } else {
                r.setTotalCreditos(r.getTotalCreditos().add(t.getMonto()));
            }

            r.setCantidadTransacciones(r.getCantidadTransacciones() + 1);
            if (t.isAnomalia()) {
                r.setCantidadAnomalias(r.getCantidadAnomalias() + 1);
            }
        }

        resumenDiarioRepository.saveAll(porFecha.values());

        log.info("--------------------- RESUMEN DIARIO ---------------------");
        porFecha.values().forEach(r ->
                log.info("{} | debitos={} | creditos={} | transacciones={} | anomalias={}",
                        r.getFecha(), r.getTotalDebitos(), r.getTotalCreditos(),
                        r.getCantidadTransacciones(), r.getCantidadAnomalias()));
        log.info("Se escribieron {} filas en resumen_diario", porFecha.size());
        log.info("----------------------------------------------------------");

        contribution.incrementWriteCount(porFecha.size());
        return RepeatStatus.FINISHED;
    }
}
