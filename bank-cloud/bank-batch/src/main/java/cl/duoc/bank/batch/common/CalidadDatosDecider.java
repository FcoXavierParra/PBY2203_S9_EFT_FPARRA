package cl.duoc.bank.batch.common;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Politica de finalizacion y re-ejecucion del Job.
 *
 * Se intercala despues del Step de carga y decide, mirando el resultado real de
 * ese Step, si el Job sigue, se reintenta o se corta. Tres salidas:
 *
 *  - REINTENTAR : el Step FALLO. Se vuelve a ejecutar, hasta MAX_INTENTOS veces.
 *                 Tiene sentido solo para fallos transitorios (BD caida, lock,
 *                 timeout); un fallo por dato sucio ya lo absorbio el
 *                 SkipPolicy antes de llegar aqui.
 *  - FALLIDO    : el Step termino "bien" pero el resultado no es confiable.
 *                 El Job termina FAILED a proposito.
 *  - CONTINUAR  : todo en orden, se pasa al Step siguiente.
 *
 * Por que FALLIDO y no CONTINUAR cuando hay demasiados descartes: un Job que
 * termina COMPLETED habiendo escrito 2 de 800 filas es peor que uno que falla,
 * porque nadie lo revisa. Los Steps de agregacion que vienen despues
 * (resumen diario, estado de cuenta anual) producirian un informe que parece
 * valido y no lo es.
 *
 * El contador de intentos vive en el ExecutionContext del JobExecution, no en
 * un campo: el bean es singleton y compartido por los tres Jobs, un campo se
 * arrastraria entre corridas. Ademas es lo que acota el ciclo
 * decider -> step -> decider y evita el bucle infinito.
 */
@Slf4j
@Component
public class CalidadDatosDecider implements JobExecutionDecider {

    /** Salidas del decider. Deben coincidir con los .on(...) de los JobConfig. */
    public static final String REINTENTAR = "REINTENTAR";
    public static final String CONTINUAR = "CONTINUAR";
    public static final String FALLIDO = "FALLIDO";

    private static final String CLAVE_INTENTOS = "calidadDatos.intentos";

    /**
     * EFT: marca que deja el decider en el ExecutionContext del Job cuando lo
     * da por FALLIDO por la CALIDAD de los datos, no por una falla tecnica. La
     * lee ReejecucionAutomatica: un archivo que viene mal sigue viniendo mal
     * cinco segundos despues, asi que relanzarlo solo gastaria tiempo.
     */
    public static final String CLAVE_RECHAZO_CALIDAD = "calidadDatos.rechazado";

    /** Reintentos del Step completo, ademas del retry por chunk. */
    private final int maxIntentos;

    /** Proporcion maxima de filas descartadas tolerada, sobre el total leido. */
    private final double toleranciaDescartes;

    public CalidadDatosDecider(
            @Value("${bank.politicas.max-reintentos-step:2}") int maxIntentos,
            @Value("${bank.politicas.tolerancia-descartes:0.30}") double toleranciaDescartes) {
        this.maxIntentos = maxIntentos;
        this.toleranciaDescartes = toleranciaDescartes;
    }

    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {

        if (stepExecution == null) {
            log.warn("El decider se invoco sin StepExecution previo. Se continua.");
            return new FlowExecutionStatus(CONTINUAR);
        }

        String step = stepExecution.getStepName();

        // --- 1. El Step fallo: reintentar mientras queden intentos ----------
        boolean fallo = stepExecution.getStatus() == BatchStatus.FAILED
                || ExitStatus.FAILED.getExitCode().equals(stepExecution.getExitStatus().getExitCode());

        if (fallo) {
            int intentos = jobExecution.getExecutionContext().getInt(CLAVE_INTENTOS, 0);
            if (intentos < maxIntentos) {
                jobExecution.getExecutionContext().putInt(CLAVE_INTENTOS, intentos + 1);
                log.warn("[POLITICA] Step '{}' fallo. Reintento {} de {}.",
                        step, intentos + 1, maxIntentos);
                return new FlowExecutionStatus(REINTENTAR);
            }
            log.error("[POLITICA] Step '{}' fallo y se agotaron los {} reintentos. El Job termina FAILED.",
                    step, maxIntentos);
            return new FlowExecutionStatus(FALLIDO);
        }

        // --- 2. Termino sin fallar: se evalua si el resultado sirve ---------
        long leidos = stepExecution.getReadCount();
        long escritos = stepExecution.getWriteCount();
        long filtrados = stepExecution.getFilterCount();
        long descartados = stepExecution.getReadSkipCount()
                + stepExecution.getProcessSkipCount()
                + stepExecution.getWriteSkipCount();

        long totalFilas = leidos + stepExecution.getReadSkipCount();

        if (totalFilas > 0 && escritos == 0) {
            log.error("[POLITICA] Step '{}' no escribio ninguna fila de {} leidas. "
                    + "Agregar sobre cero registros daria un informe vacio disfrazado de valido. "
                    + "El Job termina FAILED.", step, totalFilas);
            jobExecution.getExecutionContext().putString(CLAVE_RECHAZO_CALIDAD, "sin filas escritas");
            return new FlowExecutionStatus(FALLIDO);
        }

        if (totalFilas > 0) {
            double proporcion = (double) descartados / (double) totalFilas;
            if (proporcion > toleranciaDescartes) {
                log.error("[POLITICA] Step '{}' descarto {} de {} filas ({}%), sobre la tolerancia "
                                + "de {}%. El archivo de origen viene mal. El Job termina FAILED.",
                        step, descartados, totalFilas,
                        Math.round(proporcion * 100), Math.round(toleranciaDescartes * 100));
                jobExecution.getExecutionContext().putString(CLAVE_RECHAZO_CALIDAD,
                        "descartes " + Math.round(proporcion * 100) + "% sobre la tolerancia");
                return new FlowExecutionStatus(FALLIDO);
            }
            log.info("[POLITICA] Step '{}' dentro de tolerancia: {} descartadas de {} ({}% <= {}%). "
                            + "leidas={} filtradas={} escritas={}. Se continua.",
                    step, descartados, totalFilas,
                    Math.round(proporcion * 100), Math.round(toleranciaDescartes * 100),
                    leidos, filtrados, escritos);
        }

        return new FlowExecutionStatus(CONTINUAR);
    }
}
