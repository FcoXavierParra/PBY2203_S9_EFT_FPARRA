package cl.duoc.bank.batch.intereses;

import cl.duoc.bank.batch.common.RegistroInvalidoException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reglas de negocio del Job 2.
 *
 * Decisiones tomadas sobre los datos sucios de intereses.csv:
 *
 *  - tipo fuera de {ahorro, prestamo}: se SALTA la fila y se loguea como mal
 *    clasificada. Es el caso de la cuenta 105 ('hipoteca'). No se reclasifica
 *    a la fuerza porque no hay forma de saber si una hipoteca deberia pagar la
 *    tasa de ahorro o la de prestamo; inventar esa tasa seria peor que omitir.
 *
 *  - duplicado aparente (mismo nombre, saldo, edad y tipo): la fila se
 *    CONSERVA y se marca. Es el caso de las cuentas 101 y 106. Son cuenta_id
 *    distintos, o sea cuentas distintas para el banco; descartar una implicaria
 *    dejar a un cliente sin su interes por una sospecha.
 *
 *  - saldo 0 (cuenta 104): interes 0. No es un error, es aritmetica.
 *
 *  - edad >= EDAD_LIMITE (cuenta 108, 80 anios): se marca para revision, sin
 *    alterar el calculo.
 *
 * Todo el calculo va en BigDecimal con redondeo HALF_UP a 2 decimales.
 */
@Slf4j
public class InteresProcessor implements ItemProcessor<CuentaCsv, Cuenta> {

    private static final Set<String> TIPOS_VALIDOS = Set.of("ahorro", "prestamo");
    private static final int EDAD_LIMITE = 80;
    private static final int ESCALA_MONTO = 2;

    private final BigDecimal tasaAhorro;
    private final BigDecimal tasaPrestamo;

    /**
     * clave nombre|saldo|edad|tipo -> cuenta_id que la reclamo primero.
     *
     * Guardar el duenio y no solo la clave es indispensable: cuando el Step es
     * fault-tolerant y una fila se salta, Spring Batch hace rollback del chunk
     * y lo REPROCESA fila por fila para aislar la defectuosa. Con un simple
     * Set, esa segunda pasada encontraria las claves ya presentes y marcaria
     * como duplicadas cuentas que solo se estaban reprocesando.
     *
     * Comparando el duenio, reprocesar la cuenta 101 encuentra duenio=101 y no
     * la marca; la cuenta 106 encuentra duenio=101 y si es un duplicado real.
     *
     * ConcurrentHashMap y no HashMap: en la semana 2 el Step corre con tres
     * hilos que llaman a process() a la vez sobre esta misma instancia. Un
     * HashMap bajo escritura concurrente puede corromperse, y su putIfAbsent no
     * es atomico. Cual de dos cuentas gemelas queda como "primera" depende del
     * orden de los hilos; el par se detecta igual, que es lo que importa.
     */
    private final Map<String, Long> primeraOcurrencia = new ConcurrentHashMap<>();

    public InteresProcessor(BigDecimal tasaAhorro, BigDecimal tasaPrestamo) {
        this.tasaAhorro = tasaAhorro;
        this.tasaPrestamo = tasaPrestamo;
    }

    @Override
    public Cuenta process(CuentaCsv csv) {

        // --- 1. Validaciones ---------------------------------------------
        if (csv.getSaldo() == null) {
            throw new RegistroInvalidoException("saldo nulo en la cuenta " + csv.getCuentaId());
        }
        if (csv.getSaldo().compareTo(BigDecimal.ZERO) < 0) {
            throw new RegistroInvalidoException(
                    "saldo negativo (" + csv.getSaldo() + ") en la cuenta " + csv.getCuentaId());
        }

        String tipo = csv.getTipo() == null ? "" : csv.getTipo().trim().toLowerCase();
        if (!TIPOS_VALIDOS.contains(tipo)) {
            throw new RegistroInvalidoException(
                    "tipo mal clasificado '" + csv.getTipo() + "' en la cuenta " + csv.getCuentaId()
                            + " (esperado: ahorro o prestamo). No se calcula interes.");
        }

        // --- 2. Calculo del interes --------------------------------------
        BigDecimal tasa = "ahorro".equals(tipo) ? tasaAhorro : tasaPrestamo;

        BigDecimal saldoInicial = csv.getSaldo().setScale(ESCALA_MONTO, RoundingMode.HALF_UP);
        BigDecimal interes = saldoInicial.multiply(tasa).setScale(ESCALA_MONTO, RoundingMode.HALF_UP);

        // En ambos tipos el saldo crece: en ahorro porque el banco paga, en
        // prestamo porque la deuda devenga interes.
        BigDecimal saldoFinal = saldoInicial.add(interes);

        Cuenta cuenta = new Cuenta();
        cuenta.setCuentaId(csv.getCuentaId());
        cuenta.setNombre(csv.getNombre());
        cuenta.setSaldoInicial(saldoInicial);
        cuenta.setEdad(csv.getEdad());
        cuenta.setTipo(tipo);
        cuenta.setTasaAplicada(tasa);
        cuenta.setInteresCalculado(interes);
        cuenta.setSaldoFinal(saldoFinal);

        // --- 3. Observaciones --------------------------------------------
        List<String> notas = new ArrayList<>();

        String clave = csv.getNombre() + "|" + saldoInicial.toPlainString() + "|" + csv.getEdad() + "|" + tipo;
        Long duenio = primeraOcurrencia.putIfAbsent(clave, csv.getCuentaId());
        if (duenio != null && !duenio.equals(csv.getCuentaId())) {
            notas.add("posible duplicado de la cuenta " + duenio + " (" + clave + ")");
        }
        if (saldoInicial.compareTo(BigDecimal.ZERO) == 0) {
            notas.add("saldo inicial en cero, interes 0");
        }
        if (csv.getEdad() != null && csv.getEdad() >= EDAD_LIMITE) {
            notas.add("edad " + csv.getEdad() + " alcanza el limite de revision (" + EDAD_LIMITE + ")");
        }

        if (!notas.isEmpty()) {
            String observacion = String.join(" ; ", notas);
            cuenta.setObservacion(observacion);
            log.warn("[OBSERVACION] cuenta {} -> {}", csv.getCuentaId(), observacion);
        }

        log.debug("cuenta {} ({}) saldo {} x tasa {} = interes {} -> saldo final {}",
                cuenta.getCuentaId(), tipo, saldoInicial, tasa, interes, saldoFinal);

        return cuenta;
    }
}
