package cl.duoc.bank.batch.common;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Politica de skip compartida por los tres Jobs. Primera de las dos capas de
 * control de calidad.
 *
 * Decide, excepcion por excepcion, si una fila defectuosa se puede saltar o si
 * debe hacer fallar el Job completo. El criterio es:
 *
 *  - errores de PARSEO o de REGLA DE NEGOCIO -> se saltan, hasta el limite.
 *    Son datos sucios, que es justamente lo que el ejercicio pide manejar.
 *  - cualquier otra excepcion (fallo de conexion, bug, etc.) -> NO se salta.
 *    Tragarse esos errores esconderia un problema real de infraestructura.
 *
 * Sobre el limite y por que ahora es configurable
 * -----------------------------------------------
 * En la semana 1 el limite era la constante 10, y con datasets de 8 a 10 filas
 * funcionaba. Al correr semana_3 (1000 filas) quedo claro que un limite
 * ABSOLUTO fijo no sirve para "manejar grandes volumenes": el Step abortaba en
 * la fila 15 y nunca se llegaba a medir nada.
 *
 * El problema de fondo es que SkipPolicy solo recibe el contador de saltos, no
 * el total de filas del archivo: aqui no hay forma de calcular un porcentaje.
 * Por eso el control queda repartido en dos capas con responsabilidades
 * distintas:
 *
 *   1. Esta clase: interruptor de seguridad DURANTE el Step. Corta en un numero
 *      absoluto, sin saber cuantas filas quedan. Su unico trabajo es evitar que
 *      un archivo completamente corrupto haga girar el Job en vano.
 *   2. CalidadDatosDecider: control PROPORCIONAL DESPUES del Step. Ahi si se
 *      conocen readCount y skipCount, y se puede decidir con un porcentaje si
 *      el resultado es confiable.
 *
 * Separarlo asi es lo que permite subir el interruptor para un archivo grande
 * sin aflojar el criterio de calidad, que sigue siendo un porcentaje.
 */
@Slf4j
@Component
public class BankSkipPolicy implements SkipPolicy {

    /** Maximo de filas defectuosas toleradas por Step. */
    private final int limiteSkip;

    public BankSkipPolicy(@Value("${bank.politicas.limite-skip:100}") int limiteSkip) {
        this.limiteSkip = limiteSkip;
    }

    @Override
    public boolean shouldSkip(Throwable t, long skipCount) throws SkipLimitExceededException {

        boolean esDatoSucio =
                t instanceof FlatFileParseException
                        || t instanceof RegistroInvalidoException
                        || t instanceof NumberFormatException
                        || t instanceof java.time.format.DateTimeParseException;

        if (!esDatoSucio) {
            log.error("Excepcion NO recuperable, el Step va a fallar: {}", t.toString());
            return false;
        }

        if (skipCount >= limiteSkip) {
            log.error("Se alcanzo el limite de {} filas saltadas. Se aborta el Step. "
                            + "Si el archivo es grande y esto era esperable, subir "
                            + "bank.politicas.limite-skip.",
                    limiteSkip);
            return false;
        }

        return true;
    }
}
