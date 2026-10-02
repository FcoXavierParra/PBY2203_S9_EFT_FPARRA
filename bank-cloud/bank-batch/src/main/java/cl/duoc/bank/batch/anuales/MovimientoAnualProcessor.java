package cl.duoc.bank.batch.anuales;

import cl.duoc.bank.batch.common.RegistroInvalidoException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Reglas de negocio del Job 3.
 *
 * A diferencia de los otros dos Jobs, aqui casi nada se descarta: un estado de
 * cuenta anual tiene que reflejar TODO lo que paso en la cuenta, incluidos los
 * movimientos raros. Por eso el criterio es marcar y dejar pasar.
 *
 *  - tipo de transaccion desconocido -> se salta. Sin saber si suma o resta no
 *    se puede clasificar el movimiento.
 *  - monto 0 (cuenta 107) -> se conserva y se marca montoCero. Aparece en el
 *    informe con cantidad, pero no altera los totales.
 *  - montos negativos en retiro y compra -> son LEGITIMOS. Se respeta el signo.
 *    Corregirlo a positivo inflaria el saldo neto.
 *  - signo incoherente con el tipo (un deposito negativo, por ejemplo) -> se
 *    conserva con el signo original y se deja observacion.
 */
@Slf4j
public class MovimientoAnualProcessor implements ItemProcessor<MovimientoAnualCsv, MovimientoAnual> {

    private static final Set<String> TIPOS_VALIDOS = Set.of("deposito", "retiro", "compra");

    @Override
    public MovimientoAnual process(MovimientoAnualCsv csv) {

        if (csv.getFecha() == null) {
            throw new RegistroInvalidoException("fecha nula en movimiento de la cuenta " + csv.getCuentaId());
        }
        if (csv.getMonto() == null) {
            throw new RegistroInvalidoException("monto nulo en movimiento de la cuenta " + csv.getCuentaId());
        }

        String tipo = csv.getTransaccion() == null ? "" : csv.getTransaccion().trim().toLowerCase();
        if (!TIPOS_VALIDOS.contains(tipo)) {
            throw new RegistroInvalidoException(
                    "tipo de transaccion desconocido '" + csv.getTransaccion() + "' en la cuenta "
                            + csv.getCuentaId() + " (esperado: deposito, retiro o compra)");
        }

        MovimientoAnual m = new MovimientoAnual();
        m.setCuentaId(csv.getCuentaId());
        m.setFecha(csv.getFecha());
        m.setTipoTransaccion(tipo);
        m.setMonto(csv.getMonto());          // signo intacto, a proposito
        m.setDescripcion(csv.getDescripcion());

        List<String> notas = new ArrayList<>();

        int signo = csv.getMonto().compareTo(BigDecimal.ZERO);

        if (signo == 0) {
            m.setMontoCero(true);
            notas.add("movimiento de monto cero, sin efecto en el saldo");
        } else if ("deposito".equals(tipo) && signo < 0) {
            notas.add("deposito con monto negativo, signo inesperado (se conserva el original)");
        } else if (!"deposito".equals(tipo) && signo > 0) {
            notas.add(tipo + " con monto positivo, signo inesperado (se conserva el original)");
        }

        if (!notas.isEmpty()) {
            String observacion = String.join(" ; ", notas);
            m.setObservacion(observacion);
            log.warn("[OBSERVACION] cuenta {} {} {} -> {}",
                    csv.getCuentaId(), csv.getFecha(), tipo, observacion);
        }

        return m;
    }
}
