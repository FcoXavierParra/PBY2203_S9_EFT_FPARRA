package cl.duoc.bank.batch.common;

/**
 * Se lanza desde un ItemProcessor cuando una fila es estructuralmente valida
 * (el reader pudo parsearla) pero viola una regla de negocio que impide
 * procesarla: por ejemplo un 'tipo' mal clasificado.
 *
 * Estas excepciones las captura el SkipPolicy: la fila se salta, se registra
 * el motivo en el log y el Step continua con la siguiente.
 *
 * Ojo con la diferencia frente a devolver null en el processor:
 *  - devolver null  -> la fila se FILTRA (filterCount). Es una decision normal
 *                      de negocio, no un error.
 *  - lanzar esta    -> la fila se SALTA (skipCount). Es un dato defectuoso.
 */
public class RegistroInvalidoException extends RuntimeException {

    private final String motivo;

    public RegistroInvalidoException(String motivo) {
        super(motivo);
        this.motivo = motivo;
    }

    public String getMotivo() {
        return motivo;
    }
}
