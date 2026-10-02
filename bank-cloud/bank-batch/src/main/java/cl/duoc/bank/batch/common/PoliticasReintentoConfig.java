package cl.duoc.bank.batch.common;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;

/**
 * Espera entre reintentos de un chunk.
 *
 * La semana 2 tenia retryLimit(3) sobre TransientDataAccessException, pero sin
 * BackOffPolicy. Eso significa que los tres reintentos salen uno detras de otro
 * sin pausa, en cosa de microsegundos, y ahi esta el problema: los fallos que
 * justifican reintentar son precisamente los que necesitan TIEMPO para
 * resolverse. Un deadlock necesita que la otra transaccion haga commit; un pool
 * agotado necesita que alguien devuelva una conexion; una base en la nube que
 * viene lenta necesita respirar. Reintentar de inmediato tres veces es hacer la
 * misma pregunta tres veces en el mismo instante: se consumen los tres intentos
 * antes de que la condicion pueda haber cambiado, y ademas se agrega carga
 * justo cuando el sistema esta en aprietos.
 *
 * El crecimiento exponencial ataca las dos cosas. Con los valores por defecto
 * las esperas son 200 ms, 400 ms y 800 ms: da margen real para que el conflicto
 * se despeje y separa a los hilos que chocaron, porque cada uno reintenta en un
 * momento distinto en vez de volver a colisionar todos a la vez.
 *
 * El tope de maxInterval existe para que la espera no se dispare si algun dia
 * se sube retryLimit: sin tope, el intento numero 10 esperaria mas de un minuto
 * y medio, y un batch que deberia fallar rapido quedaria colgado pareciendo
 * vivo.
 */
@Slf4j
@Configuration
public class PoliticasReintentoConfig {

    @Bean
    public BackOffPolicy backOffPolicy(
            @Value("${bank.politicas.backoff-inicial-ms:200}") long intervaloInicial,
            @Value("${bank.politicas.backoff-multiplicador:2.0}") double multiplicador,
            @Value("${bank.politicas.backoff-maximo-ms:5000}") long intervaloMaximo) {

        ExponentialBackOffPolicy politica = new ExponentialBackOffPolicy();
        politica.setInitialInterval(intervaloInicial);
        politica.setMultiplier(multiplicador);
        politica.setMaxInterval(intervaloMaximo);

        log.info("Politica de espera entre reintentos: exponencial desde {} ms, x{}, tope {} ms",
                intervaloInicial, multiplicador, intervaloMaximo);

        return politica;
    }
}
