package cl.duoc.bank.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Punto de entrada. No se usa @EnableBatchProcessing: con Spring Boot 3 la
 * autoconfiguracion de Spring Batch ya registra el JobRepository, el
 * JobLauncher y el runner que dispara los Jobs al arrancar. Anotarlo
 * desactivaria esa autoconfiguracion.
 */
@SpringBootApplication
public class BankBatchApplication {

    /**
     * El System.exit(SpringApplication.exit(...)) no es adorno.
     *
     * Con SpringApplication.run(...) a secas, el proceso devuelve 0 aunque el
     * Job termine FAILED: Spring Boot solo consulta los ExitCodeGenerator
     * cuando se le pide el codigo explicitamente. En un batch eso es grave,
     * porque quien lo invoca -un script de evidencia, un cron, un pipeline- se
     * queda creyendo que todo salio bien.
     *
     * JobCompletionListener implementa ExitCodeGenerator y devuelve 1 cuando el
     * Job no termino COMPLETED, asi que con esta linea el codigo de salida del
     * proceso refleja el resultado real del Job.
     */
    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(BankBatchApplication.class, args)));
    }
}
