package cl.duoc.bank.batch.anuales;

import cl.duoc.bank.batch.common.BankSkipPolicy;
import cl.duoc.bank.batch.common.CalidadDatosDecider;
import cl.duoc.bank.batch.common.CamposCsv;
import cl.duoc.bank.batch.common.ConteoStepListener;
import cl.duoc.bank.batch.common.ErrorFileStepExecutionListener;
import cl.duoc.bank.batch.common.EscaladoConfig;
import cl.duoc.bank.batch.common.FabricaParticiones;
import cl.duoc.bank.batch.common.JobCompletionListener;
import cl.duoc.bank.batch.common.LectoresConcurrentes;
import cl.duoc.bank.batch.common.LoggingSkipListener;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Job 3 - Generacion de estados de cuenta anuales.
 *
 * Tres Steps y un decider:
 *   1. limpiarAnualesStep         : vacia las tablas anuales (tasklet)
 *   2. leerMovimientosAnualesStep : CSV -> validacion -> movimiento_anual
 *   3. calidadDatosDecider        : continuar, reintentar o fallar
 *   4. estadoCuentaAnualStep      : agrupa por cuenta+ano -> estado_cuenta_anual
 *                                   + informe de texto (tasklet)
 *
 * Es el Job donde el decider mas importa: el informe de auditoria del paso 4 es
 * un archivo que alguien va a firmar. Si el paso 2 cargo la mitad de los
 * movimientos, ese informe queda con totales incorrectos y con aspecto de
 * valido. Mejor no generarlo.
 */
@Configuration
public class AnualesJobConfig {

    private static final int TAMANO_CHUNK = 5;
    private static final int LIMITE_REINTENTOS = 3;

    private static Resource recurso(String dataset) {
        return new ClassPathResource("data/" + dataset + "/estados_financieros_anuales.csv");
    }

    // --------------------------------------------------------------- LECTORES

    /** Lector del modo MULTIHILO: uno solo, compartido y sincronizado. */
    @Bean
    public SynchronizedItemStreamReader<MovimientoAnualCsv> movimientoAnualReaderMultihilo(
            @Value("${bank.dataset:fin_legacy_data/semana_3}") String dataset) {

        return LectoresConcurrentes.sincronizar(construirLector(recurso(dataset), null, null));
    }

    /**
     * Lector del modo PARTICIONES: uno por particion, sobre su propio rango.
     *
     * ATENCION - este es el unico de los tres Jobs cuyo lector particionado
     * conserva saveState(false), y no es un descuido copiado de la semana 2.
     *
     * En los Jobs 1 y 2 el writer hace merge por clave, o sea es idempotente, y
     * por eso ahi el particionado puede activar saveState(true) y ganar
     * reiniciabilidad. Aca no: el writer usa persist con id generado, de modo
     * que reprocesar una fila la inserta de nuevo. La estrategia de recuperacion
     * de este Job es otra, y esta en el flujo: ante REINTENTAR se vuelve a
     * limpiarAnualesStep, que trunca las tablas antes de recargar.
     *
     * Esas dos cosas son incompatibles. Si cada particion guardara su avance, un
     * reintento truncaria las tablas y luego cada particion retomaria desde
     * donde iba -o se saltaria entera por estar COMPLETED-, dejando en la base
     * solo el tramo posterior al checkpoint. El informe de auditoria saldria con
     * menos movimientos de los que hay y con aspecto de correcto. Renunciar al
     * checkpoint aqui es lo que garantiza que el truncado y la recarga siempre
     * cubran el archivo completo.
     */
    @Bean
    @StepScope
    public FlatFileItemReader<MovimientoAnualCsv> movimientoAnualReaderParticion(
            @Value("${bank.dataset:fin_legacy_data/semana_3}") String dataset,
            @Value("#{stepExecutionContext['inicio']}") Integer inicio,
            @Value("#{stepExecutionContext['fin']}") Integer fin) {

        return construirLector(recurso(dataset), inicio, fin);
    }

    /**
     * encoding UTF-8 es obligatorio aqui: el archivo trae "Ingreso navideno"
     * con enie y "Ingreso de fin de ano" con tilde. Con el encoding por
     * defecto de la JVM esas descripciones se persisten corruptas.
     *
     * Con el dataset de la semana 2 la fila de la cuenta 105 trae la fecha como
     * 2024/10/01; CamposCsv.fecha() la normaliza en vez de descartarla.
     */
    private static FlatFileItemReader<MovimientoAnualCsv> construirLector(
            Resource recurso, Integer inicio, Integer fin) {

        FlatFileItemReaderBuilder<MovimientoAnualCsv> builder =
                new FlatFileItemReaderBuilder<MovimientoAnualCsv>()
                        .name("movimientoAnualReader")
                        .resource(recurso)
                        .encoding("UTF-8")
                        .linesToSkip(1)   // cabecera cuenta_id,fecha,transaccion,monto,descripcion
                        // Ver el javadoc del lector particionado: en este Job la
                        // recuperacion es truncar y recargar, no retomar.
                        .saveState(false)
                        .delimited()
                        .delimiter(",")
                        .names("cuenta_id", "fecha", "transaccion", "monto", "descripcion")
                        .fieldSetMapper(fs -> new MovimientoAnualCsv(
                                CamposCsv.enteroLargo(fs, "cuenta_id"),
                                CamposCsv.fecha(fs, "fecha"),
                                CamposCsv.opcional(fs, "transaccion"),
                                CamposCsv.decimal(fs, "monto"),
                                // La descripcion si puede venir vacia (cuenta 103
                                // en semana_2): es texto de apoyo, no afecta los
                                // totales del estado de cuenta.
                                CamposCsv.opcional(fs, "descripcion")));

        if (inicio == null || fin == null) {
            return builder.build();
        }
        return builder
                .currentItemCount(inicio)
                .maxItemCount(fin)
                .build();
    }

    // ------------------------------------------------------------- PROCESSOR

    @Bean
    @StepScope
    public MovimientoAnualProcessor movimientoAnualProcessor() {
        return new MovimientoAnualProcessor();
    }

    // ---------------------------------------------------------------- WRITER

    @Bean
    public JpaItemWriter<MovimientoAnual> movimientoAnualWriter(EntityManagerFactory emf) {
        JpaItemWriter<MovimientoAnual> writer = new JpaItemWriter<>();
        writer.setEntityManagerFactory(emf);
        // persist en vez de merge: las filas son nuevas y no tienen id todavia,
        // asi se evita el SELECT previo que haria merge por cada movimiento.
        writer.setUsePersist(true);
        return writer;
    }

    // ----------------------------------------------------------------- STEPS

    /**
     * allowStartIfComplete(true) es lo que hace idempotente al Job.
     *
     * El writer del paso 2 usa persist con id generado, o sea que reprocesar el
     * archivo inserta los movimientos otra vez. Como los lectores de este Job
     * van con saveState(false), un reintento del paso 2 relee el archivo
     * completo. Si el truncado no volviera a ejecutarse -y por defecto Spring
     * Batch se salta los Steps ya COMPLETED- el reintento duplicaria todos los
     * movimientos. Con esta bandera el truncado corre siempre antes de recargar.
     */
    @Bean
    public Step limpiarAnualesStep(JobRepository jobRepository,
                                   PlatformTransactionManager transactionManager,
                                   LimpiarAnualesTasklet limpiarAnualesTasklet,
                                   ConteoStepListener conteoListener) {

        return new StepBuilder("limpiarAnualesStep", jobRepository)
                .tasklet((Tasklet) limpiarAnualesTasklet, transactionManager)
                .allowStartIfComplete(true)
                .listener((StepExecutionListener) conteoListener)
                .build();
    }

    /** Configuracion de chunk comun a los dos modos. Ver TransaccionesJobConfig. */
    private SimpleStepBuilder<MovimientoAnualCsv, MovimientoAnual> chunkBase(
            String nombre,
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            ItemStreamReader<MovimientoAnualCsv> reader,
            MovimientoAnualProcessor processor,
            JpaItemWriter<MovimientoAnual> writer,
            BankSkipPolicy skipPolicy,
            BackOffPolicy backOffPolicy,
            LoggingSkipListener skipListener,
            ConteoStepListener conteoListener,
            ErrorFileStepExecutionListener errorFileListener) {

        return new StepBuilder(nombre, jobRepository)
                .<MovimientoAnualCsv, MovimientoAnual>chunk(TAMANO_CHUNK, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .faultTolerant()
                .skipPolicy(skipPolicy)
                .retryLimit(LIMITE_REINTENTOS)
                .retry(TransientDataAccessException.class)
                .backOffPolicy(backOffPolicy)
                .listener(skipListener)
                .listener(errorFileListener)
                .listener((StepExecutionListener) conteoListener)
                .listener((ChunkListener) conteoListener);
    }

    @Bean
    public Step leerMovimientosMultihiloStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            SynchronizedItemStreamReader<MovimientoAnualCsv> movimientoAnualReaderMultihilo,
            MovimientoAnualProcessor movimientoAnualProcessor,
            JpaItemWriter<MovimientoAnual> movimientoAnualWriter,
            BankSkipPolicy skipPolicy,
            BackOffPolicy backOffPolicy,
            LoggingSkipListener skipListener,
            ConteoStepListener conteoListener,
            ErrorFileStepExecutionListener errorFileListener,
            @Qualifier("batchTaskExecutor") ThreadPoolTaskExecutor batchTaskExecutor) {

        return chunkBase("leerMovimientosAnualesStep", jobRepository, transactionManager,
                movimientoAnualReaderMultihilo, movimientoAnualProcessor, movimientoAnualWriter,
                skipPolicy, backOffPolicy, skipListener, conteoListener, errorFileListener)
                .taskExecutor(batchTaskExecutor)
                .build();
    }

    /**
     * Worker de particion. allowStartIfComplete(true) por el mismo motivo que
     * en limpiarAnualesStep: ante un REINTENTAR el flujo vuelve al truncado y
     * todas las particiones tienen que volver a cargar su rango completo,
     * incluidas las que ya habian terminado bien.
     */
    @Bean
    public Step leerMovimientosWorkerStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            FlatFileItemReader<MovimientoAnualCsv> movimientoAnualReaderParticion,
            MovimientoAnualProcessor movimientoAnualProcessor,
            JpaItemWriter<MovimientoAnual> movimientoAnualWriter,
            BankSkipPolicy skipPolicy,
            BackOffPolicy backOffPolicy,
            LoggingSkipListener skipListener,
            ConteoStepListener conteoListener,
            ErrorFileStepExecutionListener errorFileListener) {

        return chunkBase("leerMovimientosWorkerStep", jobRepository, transactionManager,
                movimientoAnualReaderParticion, movimientoAnualProcessor, movimientoAnualWriter,
                skipPolicy, backOffPolicy, skipListener, conteoListener, errorFileListener)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public Step leerMovimientosAnualesStep(
            EscaladoConfig escalado,
            FabricaParticiones fabricaParticiones,
            Step leerMovimientosMultihiloStep,
            Step leerMovimientosWorkerStep,
            @Value("${bank.dataset:fin_legacy_data/semana_3}") String dataset) {

        if (escalado.esParticionado()) {
            return fabricaParticiones.particionar(
                    "leerMovimientosAnualesStep", leerMovimientosWorkerStep, recurso(dataset));
        }
        return leerMovimientosMultihiloStep;
    }

    /**
     * Monohilo: agrupa por cuenta y ano, y escribe el informe de texto. Un
     * archivo escrito por varios hilos a la vez saldria con las lineas
     * entremezcladas.
     */
    @Bean
    public Step estadoCuentaAnualStep(JobRepository jobRepository,
                                      PlatformTransactionManager transactionManager,
                                      EstadoCuentaAnualTasklet estadoCuentaAnualTasklet,
                                      ConteoStepListener conteoListener) {

        return new StepBuilder("estadoCuentaAnualStep", jobRepository)
                .tasklet((Tasklet) estadoCuentaAnualTasklet, transactionManager)
                .listener((StepExecutionListener) conteoListener)
                .build();
    }

    // ------------------------------------------------------------------- JOB

    @Bean
    public Job anualesJob(JobRepository jobRepository,
                          Step verificarConexionStep,
                          Step limpiarAnualesStep,
                          Step leerMovimientosAnualesStep,
                          Step estadoCuentaAnualStep,
                          CalidadDatosDecider calidadDatosDecider,
                          JobCompletionListener jobCompletionListener) {

        Flow flujo = new FlowBuilder<Flow>("anualesFlow")
                // EFT: primero se comprueba la base. Si no responde, el Job termina
                // FAILED aqui y ReejecucionAutomatica lo relanza.
                .start(verificarConexionStep)
                .next(limpiarAnualesStep)
                .next(leerMovimientosAnualesStep)
                .on("*").to(calidadDatosDecider)
                .from(calidadDatosDecider)
                    // El reintento vuelve al truncado, no directo a la carga:
                    // recargar sin vaciar antes duplicaria los movimientos.
                    .on(CalidadDatosDecider.REINTENTAR).to(limpiarAnualesStep)
                .from(calidadDatosDecider)
                    .on(CalidadDatosDecider.CONTINUAR).to(estadoCuentaAnualStep)
                .from(calidadDatosDecider)
                    .on(CalidadDatosDecider.FALLIDO).fail()
                .build();

        return new JobBuilder("anualesJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(jobCompletionListener)
                .start(flujo)
                .end()
                .build();
    }
}
