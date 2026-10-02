package cl.duoc.bank.batch.common;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

/**
 * Maneja el ciclo de vida del archivo de errores alrededor de un Step.
 *
* Abre el archivo de errores antes de que el Step lea la primera fila y lo cierra
 * despues de la ultima, sin importar si el Step termino COMPLETED o FAILED.
 * El cierre es lo que garantiza el flush: sin el, las ultimas filas saltadas se
 * quedarian en el buffer y no llegarian al disco.
 *
 * Va separado de LoggingSkipListener a proposito: ese decide QUE se escribe,
 * este decide CUANDO el archivo esta disponible. Mezclarlos obligaria a abrir
 * el archivo de forma perezosa en cada skip y a no cerrarlo nunca.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ErrorFileStepExecutionListener implements StepExecutionListener {

    private final RegistroErrores registroErrores;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.info("Abre el archivo de errores al inicio del Step '{}'", stepExecution.getStepName());
        registroErrores.abrir(stepExecution.getStepName());
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        int registrados = registroErrores.cerrar(stepExecution.getStepName());
        log.info("Cierra el archivo de errores al final del Step '{}' ({} fila(s) descartada(s))",
                stepExecution.getStepName(), registrados);
        // Se devuelve el ExitStatus sin tocarlo: este listener audita, no decide
        // el resultado del Step. Eso lo hace CalidadDatosDecider.
        return stepExecution.getExitStatus();
    }
}
