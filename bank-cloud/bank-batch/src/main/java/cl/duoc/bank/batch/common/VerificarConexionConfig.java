package cl.duoc.bank.batch.common;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * EFT: primer Step de los tres Jobs. Comprueba que la base responde ANTES de
 * leer una sola fila.
 *
 * Sin el, una base caida se descubre a mitad del Step de carga: con chunks ya
 * confirmados, el reintento por chunk gastando sus intentos contra una conexion
 * que no va a volver, y el control de calidad contando como "descartes" filas
 * que nunca se pudieron escribir. Fallar aqui, al principio y con una causa
 * clara, es lo que permite a ReejecucionAutomatica distinguir "la
 * infraestructura fallo, conviene esperar y volver a intentar" de "los datos
 * vienen mal, relanzar no sirve".
 *
 * SIMULACION DE FALLA CRITICA
 * ===========================
 * bank.simulacion.fallas-conexion=N hace que las primeras N verificaciones del
 * proceso fallen como si la base no respondiera. Es la forma de demostrar la
 * reejecucion automatica sin apagar Oracle de verdad. Por defecto 0: en una
 * corrida normal no se simula nada.
 */
@Slf4j
@Configuration
public class VerificarConexionConfig {

    private final AtomicInteger verificaciones = new AtomicInteger();

    @Bean
    public Step verificarConexionStep(JobRepository jobRepository,
                                      PlatformTransactionManager transactionManager,
                                      DataSource dataSource,
                                      @Value("${bank.simulacion.fallas-conexion:0}") int fallasSimuladas) {

        return new StepBuilder("verificarConexionStep", jobRepository)
                .tasklet((contribucion, contexto) -> {
                    int n = verificaciones.incrementAndGet();
                    if (n <= fallasSimuladas) {
                        log.error("[CONEXION] Verificacion {}: la base no responde (falla SIMULADA {} de {}).",
                                n, n, fallasSimuladas);
                        throw new CannotGetJdbcConnectionException(
                                "Conexion a la base caida (simulada por bank.simulacion.fallas-conexion)");
                    }
                    try (Connection c = dataSource.getConnection()) {
                        if (!c.isValid(5)) {
                            throw new CannotGetJdbcConnectionException("La base no valido la conexion en 5 s");
                        }
                        log.info("[CONEXION] Verificacion {}: base disponible ({}).",
                                n, c.getMetaData().getDatabaseProductName());
                    } catch (SQLException e) {
                        throw new CannotGetJdbcConnectionException("No se pudo abrir conexion a la base", e);
                    }
                    return RepeatStatus.FINISHED;
                }, transactionManager)
                // Se vuelve a ejecutar en cada relanzamiento: que la base
                // respondiera la vez anterior no dice nada de ahora.
                .allowStartIfComplete(true)
                .build();
    }
}
