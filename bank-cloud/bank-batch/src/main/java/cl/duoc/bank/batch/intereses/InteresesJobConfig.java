package cl.duoc.bank.batch.intereses;

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
 * Job 2 - Calculo de intereses mensuales.
 *
 * Un Step de chunk y un decider de finalizacion. El Step de carga existe en las
 * dos variantes de escalado, igual que en el Job 1, y se elige con
 * bank.escalado.modo.
 *
 * La escritura es transaccional por chunk: JpaItemWriter hace merge, o sea
 * inserta la cuenta si no existe y actualiza saldo_final si ya estaba. Si un
 * chunk falla, ese chunk completo se revierte y no queda un saldo a medio
 * actualizar. Ese merge por clave es tambien lo que hace seguro reintentar el
 * Step: reprocesar una cuenta ya escrita la deja con el mismo saldo, no lo
 * acumula dos veces. Es la propiedad que vuelve inofensivo el reintento tanto
 * en multihilo (que rearranca el archivo entero) como en particiones (que
 * rearranca solo el rango pendiente).
 */
@Configuration
public class InteresesJobConfig {

    private static final int TAMANO_CHUNK = 5;
    private static final int LIMITE_REINTENTOS = 3;

    private static Resource recurso(String dataset) {
        return new ClassPathResource("data/" + dataset + "/intereses_trimestrales.csv");
    }

    // --------------------------------------------------------------- LECTORES

    /**
     * Lector del modo MULTIHILO: uno solo, compartido y sincronizado.
     *
     * Con el dataset de la semana 2 este lector descarta la cuenta 103 (edad
     * vacia: readInt sobre cadena vacia lanza NumberFormatException) y la 104
     * (saldo vacio). Ambas suben como FlatFileParseException y las absorbe el
     * SkipPolicy.
     */
    @Bean
    public SynchronizedItemStreamReader<CuentaCsv> cuentaReaderMultihilo(
            @Value("${bank.dataset:fin_legacy_data/semana_3}") String dataset) {

        return LectoresConcurrentes.sincronizar(construirLector(recurso(dataset), null, null));
    }

    /** Lector del modo PARTICIONES: uno por particion, sobre su propio rango. */
    @Bean
    @StepScope
    public FlatFileItemReader<CuentaCsv> cuentaReaderParticion(
            @Value("${bank.dataset:fin_legacy_data/semana_3}") String dataset,
            @Value("#{stepExecutionContext['inicio']}") Integer inicio,
            @Value("#{stepExecutionContext['fin']}") Integer fin) {

        return construirLector(recurso(dataset), inicio, fin);
    }

    private static FlatFileItemReader<CuentaCsv> construirLector(
            Resource recurso, Integer inicio, Integer fin) {

        FlatFileItemReaderBuilder<CuentaCsv> builder =
                new FlatFileItemReaderBuilder<CuentaCsv>()
                        .name("cuentaReader")
                        .resource(recurso)
                        .encoding("UTF-8")
                        .linesToSkip(1)        // cabecera cuenta_id,nombre,saldo,edad,tipo
                        .delimited()
                        .delimiter(",")
                        .names("cuenta_id", "nombre", "saldo", "edad", "tipo")
                        .fieldSetMapper(fs -> new CuentaCsv(
                                CamposCsv.enteroLargo(fs, "cuenta_id"),
                                CamposCsv.obligatorio(fs, "nombre"),
                                CamposCsv.decimal(fs, "saldo"),
                                CamposCsv.entero(fs, "edad"),
                                CamposCsv.opcional(fs, "tipo")));

        if (inicio == null || fin == null) {
            return builder.saveState(false).build();
        }
        return builder
                .saveState(true)
                .currentItemCount(inicio)
                .maxItemCount(fin)
                .build();
    }

    // ------------------------------------------------------------- PROCESSOR

    /**
     * Tasas mensuales. Son placeholders configurables por properties; hay que
     * confirmarlas con el facilitador antes de la entrega final.
     *   ahorro   0,5 % mensual -> 0.005
     *   prestamo 1,5 % mensual -> 0.015
     */
    @Bean
    @StepScope
    public InteresProcessor interesProcessor(
            @Value("${bank.intereses.tasa-ahorro:0.005}") BigDecimal tasaAhorro,
            @Value("${bank.intereses.tasa-prestamo:0.015}") BigDecimal tasaPrestamo) {
        return new InteresProcessor(tasaAhorro, tasaPrestamo);
    }

    // ---------------------------------------------------------------- WRITER

    @Bean
    public JpaItemWriter<Cuenta> cuentaWriter(EntityManagerFactory emf) {
        JpaItemWriter<Cuenta> writer = new JpaItemWriter<>();
        writer.setEntityManagerFactory(emf);
        return writer;
    }

    // ----------------------------------------------------------------- STEPS

    /** Configuracion de chunk comun a los dos modos. Ver TransaccionesJobConfig. */
    private SimpleStepBuilder<CuentaCsv, Cuenta> chunkBase(
            String nombre,
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            ItemStreamReader<CuentaCsv> reader,
            InteresProcessor processor,
            JpaItemWriter<Cuenta> writer,
            BankSkipPolicy skipPolicy,
            BackOffPolicy backOffPolicy,
            LoggingSkipListener skipListener,
            ConteoStepListener conteoListener,
            ErrorFileStepExecutionListener errorFileListener) {

        return new StepBuilder(nombre, jobRepository)
                .<CuentaCsv, Cuenta>chunk(TAMANO_CHUNK, transactionManager)
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
    public Step calcularInteresesMultihiloStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            SynchronizedItemStreamReader<CuentaCsv> cuentaReaderMultihilo,
            InteresProcessor interesProcessor,
            JpaItemWriter<Cuenta> cuentaWriter,
            BankSkipPolicy skipPolicy,
            BackOffPolicy backOffPolicy,
            LoggingSkipListener skipListener,
            ConteoStepListener conteoListener,
            ErrorFileStepExecutionListener errorFileListener,
            @Qualifier("batchTaskExecutor") ThreadPoolTaskExecutor batchTaskExecutor) {

        return chunkBase("calcularInteresesStep", jobRepository, transactionManager,
                cuentaReaderMultihilo, interesProcessor, cuentaWriter,
                skipPolicy, backOffPolicy, skipListener, conteoListener, errorFileListener)
                .taskExecutor(batchTaskExecutor)
                .build();
    }

    @Bean
    public Step calcularInteresesWorkerStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            FlatFileItemReader<CuentaCsv> cuentaReaderParticion,
            InteresProcessor interesProcessor,
            JpaItemWriter<Cuenta> cuentaWriter,
            BankSkipPolicy skipPolicy,
            BackOffPolicy backOffPolicy,
            LoggingSkipListener skipListener,
            ConteoStepListener conteoListener,
            ErrorFileStepExecutionListener errorFileListener) {

        return chunkBase("calcularInteresesWorkerStep", jobRepository, transactionManager,
                cuentaReaderParticion, interesProcessor, cuentaWriter,
                skipPolicy, backOffPolicy, skipListener, conteoListener, errorFileListener)
                .build();
    }

    @Bean
    public Step calcularInteresesStep(
            EscaladoConfig escalado,
            FabricaParticiones fabricaParticiones,
            Step calcularInteresesMultihiloStep,
            Step calcularInteresesWorkerStep,
            @Value("${bank.dataset:fin_legacy_data/semana_3}") String dataset) {

        if (escalado.esParticionado()) {
            return fabricaParticiones.particionar(
                    "calcularInteresesStep", calcularInteresesWorkerStep, recurso(dataset));
        }
        return calcularInteresesMultihiloStep;
    }

    // ------------------------------------------------------------------- JOB

    /**
     * Este Job no tiene Step siguiente, asi que el decider solo decide entre
     * terminar bien, reintentar o fallar. Vale la pena igual: sin el, un archivo
     * de origen corrupto dejaria la tabla cuenta con dos filas y el Job diria
     * COMPLETED.
     */
    @Bean
    public Job interesesJob(JobRepository jobRepository,
                            Step verificarConexionStep,
                            Step calcularInteresesStep,
                            CalidadDatosDecider calidadDatosDecider,
                            JobCompletionListener jobCompletionListener) {

        Flow flujo = new FlowBuilder<Flow>("interesesFlow")
                // EFT: primero se comprueba la base. Si no responde, el Job termina
                // FAILED aqui y ReejecucionAutomatica lo relanza.
                .start(verificarConexionStep)
                .next(calcularInteresesStep)
                .on("*").to(calidadDatosDecider)
                .from(calidadDatosDecider)
                    .on(CalidadDatosDecider.REINTENTAR).to(calcularInteresesStep)
                .from(calidadDatosDecider)
                    .on(CalidadDatosDecider.CONTINUAR).end()
                .from(calidadDatosDecider)
                    .on(CalidadDatosDecider.FALLIDO).fail()
                .build();

        return new JobBuilder("interesesJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(jobCompletionListener)
                .start(flujo)
                .end()
                .build();
    }
}
