package cl.duoc.bank.batch.common;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

/**
 * Deja constancia de CADA fila saltada y del motivo, en dos destinos:
 * el log de consola y el archivo 'errores.csv'.
 *
 * Sin esto el skip seria silencioso: el Job diria COMPLETED y nadie sabria que
 * se descartaron filas ni cuales. Para una migracion bancaria eso no sirve como
 * evidencia.
 *
 * En la semana 2 se agrega el archivo porque la consola se pierde y porque, con
 * el Step multihilo, las lineas de tres hilos salen entremezcladas: el CSV, que
 * guarda el nombre del hilo en una columna, es mucho mas facil de auditar
 * despues.
 *
 * Este listener no sabe en que Step esta, y no le hace falta: el nombre lo fija
 * ErrorFileStepExecutionListener cuando abre el archivo.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LoggingSkipListener implements SkipListener<Object, Object> {

    private final RegistroErrores registroErrores;

    @Override
    public void onSkipInRead(Throwable t) {
        String causa = causaRaiz(t);

        if (t instanceof FlatFileParseException ffpe) {
            log.warn("[SKIP-LECTURA] linea {} no se pudo parsear | contenido='{}' | causa={}",
                    ffpe.getLineNumber(), ffpe.getInput(), causa);
            registroErrores.registrar("LECTURA",
                    "linea " + ffpe.getLineNumber() + ": " + causa, ffpe.getInput());
        } else {
            log.warn("[SKIP-LECTURA] causa={}", causa);
            registroErrores.registrar("LECTURA", causa, "");
        }
    }

    @Override
    public void onSkipInProcess(Object item, Throwable t) {
        String causa = causaRaiz(t);
        log.warn("[SKIP-PROCESO] registro={} | motivo={}", item, causa);
        registroErrores.registrar("PROCESO", causa, String.valueOf(item));
    }

    @Override
    public void onSkipInWrite(Object item, Throwable t) {
        String causa = causaRaiz(t);
        log.warn("[SKIP-ESCRITURA] registro={} | motivo={}", item, causa);
        registroErrores.registrar("ESCRITURA", causa, String.valueOf(item));
    }

    private String causaRaiz(Throwable t) {
        Throwable actual = t;
        while (actual.getCause() != null && actual.getCause() != actual) {
            actual = actual.getCause();
        }
        return actual.getClass().getSimpleName() + ": " + actual.getMessage();
    }
}
