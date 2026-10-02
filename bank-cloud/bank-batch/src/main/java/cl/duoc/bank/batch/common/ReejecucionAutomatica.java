package cl.duoc.bank.batch.common;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * EFT: politica de finalizacion y REEJECUCION AUTOMATICA ante fallos criticos.
 *
 * Reemplaza al runner de Spring Boot (spring.batch.job.enabled=false) para
 * poder decidir que hacer cuando el Job termina FAILED. Hasta la semana 3 el
 * proyecto ya tenia dos capas de reintento: por chunk (PoliticasReintentoConfig,
 * con backoff) y por Step (CalidadDatosDecider). Las dos viven DENTRO de una
 * ejecucion del Job. Si la base se cae lo suficiente como para agotarlas, el Job
 * terminaba FAILED y nadie lo volvia a lanzar: alguien tenia que notarlo y
 * correrlo a mano.
 *
 * Esta es la tercera capa, por encima del Job:
 *
 *   FAILED por falla tecnica   -> espera (backoff exponencial) y RELANZA con los
 *                                 MISMOS parametros. Spring Batch lo trata como
 *                                 un restart de la misma JobInstance: los Steps
 *                                 que ya terminaron no se repiten (salvo los
 *                                 marcados allowStartIfComplete) y el Job sigue
 *                                 desde donde fallo.
 *   FAILED por calidad de datos -> NO relanza. Lo marca CalidadDatosDecider. Un
 *                                 archivo que viene mal sigue viniendo mal: se
 *                                 termina con codigo 1 y el motivo en el log.
 *   COMPLETED                  -> termina con codigo 0.
 *   agotados los relanzamientos -> termina con codigo 1. Es la politica de
 *                                 finalizacion: no se reintenta para siempre.
 */
@Slf4j
@Component
public class ReejecucionAutomatica implements ApplicationRunner {

    private final ApplicationContext contexto;
    private final JobLauncher jobLauncher;
    private final JobExplorer jobExplorer;
    private final String nombreJob;
    private final int maxRelanzamientos;
    private final long esperaInicialMs;
    private final double multiplicador;

    public ReejecucionAutomatica(ApplicationContext contexto,
                                 JobLauncher jobLauncher,
                                 JobExplorer jobExplorer,
                                 @Value("${spring.batch.job.name}") String nombreJob,
                                 @Value("${bank.reejecucion.max-relanzamientos:3}") int maxRelanzamientos,
                                 @Value("${bank.reejecucion.espera-inicial-ms:5000}") long esperaInicialMs,
                                 @Value("${bank.reejecucion.multiplicador:2.0}") double multiplicador) {
        this.contexto = contexto;
        this.jobLauncher = jobLauncher;
        this.jobExplorer = jobExplorer;
        this.nombreJob = nombreJob;
        this.maxRelanzamientos = maxRelanzamientos;
        this.esperaInicialMs = esperaInicialMs;
        this.multiplicador = multiplicador;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Job job = contexto.getBean(nombreJob, Job.class);

        // Parametros nuevos para la PRIMERA ejecucion (run.id siguiente). Los
        // relanzamientos reusan estos mismos: eso es lo que los vuelve un
        // restart de la misma instancia y no una corrida desde cero.
        JobParameters parametros = new JobParametersBuilder(jobExplorer)
                .getNextJobParameters(job)
                .toJobParameters();

        JobExecution ejecucion = jobLauncher.run(job, parametros);
        long espera = esperaInicialMs;

        for (int relanzamiento = 1;
             ejecucion.getStatus() == BatchStatus.FAILED && relanzamiento <= maxRelanzamientos;
             relanzamiento++) {

            String rechazo = ejecucion.getExecutionContext()
                    .getString(CalidadDatosDecider.CLAVE_RECHAZO_CALIDAD, null);
            if (rechazo != null) {
                log.error("[REEJECUCION] '{}' FAILED por calidad de datos ({}). No se relanza: "
                        + "el archivo de origen hay que corregirlo.", nombreJob, rechazo);
                return;
            }

            log.warn("[REEJECUCION] '{}' termino FAILED por una falla tecnica. Relanzamiento {} de {} en {} ms.",
                    nombreJob, relanzamiento, maxRelanzamientos, espera);
            Thread.sleep(espera);
            espera = (long) (espera * multiplicador);

            ejecucion = jobLauncher.run(job, parametros);
            log.info("[REEJECUCION] Relanzamiento {} -> {} (JobExecution id={}, misma JobInstance id={})",
                    relanzamiento, ejecucion.getStatus(), ejecucion.getId(),
                    ejecucion.getJobInstance().getInstanceId());
        }

        if (ejecucion.getStatus() == BatchStatus.FAILED) {
            log.error("[REEJECUCION] '{}' sigue FAILED tras {} relanzamientos. Se da por terminado "
                    + "(politica de finalizacion).", nombreJob, maxRelanzamientos);
        }
    }
}
