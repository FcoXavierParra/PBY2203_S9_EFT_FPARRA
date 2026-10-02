package cl.duoc.bank.batch.transacciones;

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

import java.math.BigDecimal;

/**
 * Job 1 - Reporte de transacciones diarias.
 *
 * Dos Steps y un decider:
 *   1. leerTransaccionesStep : CSV -> validacion -> tabla transaccion
 *   2. calidadDatosDecider   : decide si continuar, reintentar o fallar
 *   3. resumenDiarioStep     : tabla transaccion -> tabla resumen_diario (tasklet)
 *
 * El Step de carga existe en DOS variantes y la activa se elige con
 * bank.escalado.modo. Ambas hacen exactamente el mismo trabajo funcional -mismo
 * chunk, mismo processor, mismas politicas de skip y reintento- y difieren solo
 * en como se reparte la lectura. Es lo que permite compararlas de forma honesta
 * en el benchmark: si tambien cambiara la logica, la diferencia de tiempos no
 * seria atribuible a la tecnica de escalado.
 */
@Configuration
public class TransaccionesJobConfig {

    private static final int TAMANO_CHUNK = 5;
    private static final int LIMITE_REINTENTOS = 3;

    private static Resource recurso(String dataset) {
        return new ClassPathResource("data/" + dataset + "/movimientos_financieros_diarios.csv");
    }

    // --------------------------------------------------------------- LECTORES

    /**
     * Lector del modo MULTIHILO: uno solo, compartido por los tres hilos.
     *
     * Va envuelto en SynchronizedItemStreamReader porque FlatFileItemReader no
     * es thread-safe, y con saveState(false) porque la posicion del archivo es
     * compartida y no representa un punto consistente de reinicio. Ver
     * LectoresConcurrentes para el detalle completo.
     */
    @Bean
    public SynchronizedItemStreamReader<TransaccionCsv> transaccionReaderMultihilo(
            @Value("${bank.dataset:fin_legacy_data/semana_3}") String dataset) {

        return LectoresConcurrentes.sincronizar(construirLector(recurso(dataset), null, null));
    }

    /**
     * Lector del modo PARTICIONES: uno por particion, cada uno sobre su rango.
     *
     * Aca esta la ventaja estructural del particionado. Como cada particion
     * tiene su propio lector sobre un tramo distinto del archivo, no hay
     * recurso compartido: desaparece el candado del SynchronizedItemStreamReader
     * y con el la contencion en la lectura.
     *
     * Y saveState vuelve a true, que es lo que de verdad importa. En modo
     * multihilo el Step no es reiniciable; aca cada particion guarda su propio
     * avance, asi que una re-ejecucion retoma solo las particiones que no
     * completaron y desde donde iban. La resiliencia que pide el titulo de la
     * semana no viene de correr mas rapido, viene de esto.
     *
     * inicio y fin llegan del ExecutionContext que armo ParticionadorPorRango, y
     * se traducen a currentItemCount y maxItemCount: indices de item, con la
     * cabecera ya descontada por linesToSkip.
     */
    @Bean
    @StepScope
    public FlatFileItemReader<TransaccionCsv> transaccionReaderParticion(
            @Value("${bank.dataset:fin_legacy_data/semana_3}") String dataset,
            @Value("#{stepExecutionContext['inicio']}") Integer inicio,
            @Value("#{stepExecutionContext['fin']}") Integer fin) {

        return construirLector(recurso(dataset), inicio, fin);
    }

    /**
     * Construccion comun de los dos lectores.
     *
     * El mapeo se hace a mano en vez de con BeanWrapperFieldSetMapper para que
     * la conversion de fecha y monto sea explicita, y para poder distinguir dos
     * cosas que no son lo mismo: un dato CORREGIBLE y un dato PERDIDO.
     * CamposCsv.fecha() normaliza 2024/01/04 a 2024-01-04, porque el orden de
     * los campos es inequivoco; CamposCsv.decimal() sobre un monto vacio lanza,
     * porque no hay nada que corregir.
     *
     * Las excepciones suben envueltas en FlatFileParseException y el SkipPolicy
     * las reconoce como dato sucio.
     */
    private static FlatFileItemReader<TransaccionCsv> construirLector(
            Resource recurso, Integer inicio, Integer fin) {

        FlatFileItemReaderBuilder<TransaccionCsv> builder =
                new FlatFileItemReaderBuilder<TransaccionCsv>()
                        .name("transaccionReader")
                        .resource(recurso)
                        .encoding("UTF-8")
                        .linesToSkip(1)                       // cabecera id,fecha,monto,tipo
                        .delimited()
                        .delimiter(",")
                        .names("id", "fecha", "monto", "tipo")
                        .fieldSetMapper(fs -> new TransaccionCsv(
                                CamposCsv.enteroLargo(fs, "id"),
                                CamposCsv.fecha(fs, "fecha"),
                                CamposCsv.decimal(fs, "monto"),
                                CamposCsv.opcional(fs, "tipo")));

        if (inicio == null || fin == null) {
            // Modo multihilo: el lector recorre el archivo entero y renuncia al
            // estado, porque la posicion la comparten varios hilos.
            return builder.saveState(false).build();
        }
        return builder
                .saveState(true)
                .currentItemCount(inicio)
                .maxItemCount(fin)
                .build();
    }

    // ------------------------------------------------------------- PROCESSOR

    @Bean
    @StepScope
    public TransaccionProcessor transaccionProcessor(
            @Value("${bank.transacciones.umbral-anomalia:2500}") BigDecimal umbralAnomalia) {
        return new TransaccionProcessor(umbralAnomalia);
    }

    // ---------------------------------------------------------------- WRITER

    @Bean
    public JpaItemWriter<Transaccion> transaccionWriter(EntityManagerFactory emf) {
        JpaItemWriter<Transaccion> writer = new JpaItemWriter<>();
        writer.setEntityManagerFactory(emf);
        return writer;
    }

    // ----------------------------------------------------------------- STEPS

    /**
     * Configuracion de chunk comun a los dos modos.
     *
     * El orden de las llamadas al builder no es cosmetico. faultTolerant() debe
     * ir antes que skipPolicy(), retry() y backOffPolicy(), y taskExecutor()
     * queda fuera de este metodo porque devuelve un builder que ya no acepta las
     * politicas: si se llamara aca, el modo particionado heredaria un
     * taskExecutor que no debe tener.
     *
     * conteoListener se registra dos veces con cast explicito porque implementa
     * StepExecutionListener y ChunkListener, y cada facet se engancha por
     * separado. Sin el cast la llamada seria ambigua para el compilador.
     */
    private SimpleStepBuilder<TransaccionCsv, Transaccion> chunkBase(
            String nombre,
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            ItemStreamReader<TransaccionCsv> reader,
            TransaccionProcessor processor,
            JpaItemWriter<Transaccion> writer,
            BankSkipPolicy skipPolicy,
            BackOffPolicy backOffPolicy,
            LoggingSkipListener skipListener,
            ConteoStepListener conteoListener,
            ErrorFileStepExecutionListener errorFileListener) {

        return new StepBuilder(nombre, jobRepository)
                .<TransaccionCsv, Transaccion>chunk(TAMANO_CHUNK, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .faultTolerant()
                // Skip: filas defectuosas no botan el Job.
                .skipPolicy(skipPolicy)
                // Retry: un fallo transitorio de BD (deadlock, timeout) se
                // reintenta antes de darlo por perdido. Con varios hilos o
                // particiones escribiendo a la vez los deadlocks pasan a ser
                // una posibilidad real, no teorica.
                .retryLimit(LIMITE_REINTENTOS)
                .retry(TransientDataAccessException.class)
                // Espera creciente entre reintentos. Ver PoliticasReintentoConfig.
                .backOffPolicy(backOffPolicy)
                .listener(skipListener)
                .listener(errorFileListener)
                .listener((StepExecutionListener) conteoListener)
                .listener((ChunkListener) conteoListener);
    }

    /** Variante MULTIHILO: un Step, N hilos sacando chunks de la misma cola. */
    @Bean
    public Step leerTransaccionesMultihiloStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            SynchronizedItemStreamReader<TransaccionCsv> transaccionReaderMultihilo,
            TransaccionProcessor transaccionProcessor,
            JpaItemWriter<Transaccion> transaccionWriter,
            BankSkipPolicy skipPolicy,
            BackOffPolicy backOffPolicy,
            LoggingSkipListener skipListener,
            ConteoStepListener conteoListener,
            ErrorFileStepExecutionListener errorFileListener,
            @Qualifier("batchTaskExecutor") ThreadPoolTaskExecutor batchTaskExecutor) {

        return chunkBase("leerTransaccionesStep", jobRepository, transactionManager,
                transaccionReaderMultihilo, transaccionProcessor, transaccionWriter,
                skipPolicy, backOffPolicy, skipListener, conteoListener, errorFileListener)
                .taskExecutor(batchTaskExecutor)
                .build();
    }

    /**
     * Variante PARTICIONES, worker: procesa UNA particion, monohilo.
     *
     * No lleva taskExecutor y eso es deliberado, no un olvido. El paralelismo
     * aca lo aporta el manager al lanzar varios workers a la vez; poner ademas
     * un taskExecutor dentro de cada worker multiplicaria los hilos
     * (gridSize x hilos) y devolveria el problema de contencion en el lector
     * que el particionado justamente elimina.
     */
    @Bean
    public Step leerTransaccionesWorkerStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            FlatFileItemReader<TransaccionCsv> transaccionReaderParticion,
            TransaccionProcessor transaccionProcessor,
            JpaItemWriter<Transaccion> transaccionWriter,
            BankSkipPolicy skipPolicy,
            BackOffPolicy backOffPolicy,
            LoggingSkipListener skipListener,
            ConteoStepListener conteoListener,
            ErrorFileStepExecutionListener errorFileListener) {

        return chunkBase("leerTransaccionesWorkerStep", jobRepository, transactionManager,
                transaccionReaderParticion, transaccionProcessor, transaccionWriter,
                skipPolicy, backOffPolicy, skipListener, conteoListener, errorFileListener)
                .build();
    }

    /** Step de carga que ve el Job. Selecciona la variante segun configuracion. */
    @Bean
    public Step leerTransaccionesStep(
            EscaladoConfig escalado,
            FabricaParticiones fabricaParticiones,
            Step leerTransaccionesMultihiloStep,
            Step leerTransaccionesWorkerStep,
            @Value("${bank.dataset:fin_legacy_data/semana_3}") String dataset) {

        if (escalado.esParticionado()) {
            return fabricaParticiones.particionar(
                    "leerTransaccionesStep", leerTransaccionesWorkerStep, recurso(dataset));
        }
        return leerTransaccionesMultihiloStep;
    }

    /**
     * Step de agregacion. Monohilo a proposito: es un solo SELECT ... GROUP BY
     * seguido de un punado de inserts. Paralelizarlo no ahorraria tiempo y
     * abriria una condicion de carrera sobre resumen_diario.
     */
    @Bean
    public Step resumenDiarioStep(JobRepository jobRepository,
                                  PlatformTransactionManager transactionManager,
                                  ResumenDiarioTasklet resumenDiarioTasklet,
                                  ConteoStepListener conteoListener) {

        return new StepBuilder("resumenDiarioStep", jobRepository)
                .tasklet((Tasklet) resumenDiarioTasklet, transactionManager)
                .listener((StepExecutionListener) conteoListener)
                .build();
    }

    // ------------------------------------------------------------------- JOB

    /**
     * Flujo con politica de finalizacion y re-ejecucion.
     *
     * El on("*") despues del Step de carga es lo que permite que el flujo llegue
     * al decider incluso cuando el Step termino FAILED. Sin ese comodin, un
     * fallo cortaria el Job de inmediato y el decider nunca podria decidir
     * reintentarlo.
     */
    @Bean
    public Job transaccionesJob(JobRepository jobRepository,
                                Step verificarConexionStep,
                                Step leerTransaccionesStep,
                                Step resumenDiarioStep,
                                CalidadDatosDecider calidadDatosDecider,
                                JobCompletionListener jobCompletionListener) {

        Flow flujo = new FlowBuilder<Flow>("transaccionesFlow")
                // EFT: primero se comprueba la base. Si no responde, el Job termina
                // FAILED aqui y ReejecucionAutomatica lo relanza.
                .start(verificarConexionStep)
                .next(leerTransaccionesStep)
                .on("*").to(calidadDatosDecider)
                .from(calidadDatosDecider)
                    .on(CalidadDatosDecider.REINTENTAR).to(leerTransaccionesStep)
                .from(calidadDatosDecider)
                    .on(CalidadDatosDecider.CONTINUAR).to(resumenDiarioStep)
                .from(calidadDatosDecider)
                    .on(CalidadDatosDecider.FALLIDO).fail()
                .build();

        return new JobBuilder("transaccionesJob", jobRepository)
                // Permite re-ejecutar el Job sin cambiar los parametros a mano:
                // el incrementer agrega un run.id distinto en cada corrida.
                .incrementer(new RunIdIncrementer())
                .listener(jobCompletionListener)
                .start(flujo)
                .end()
                .build();
    }
}
