package cl.duoc.bank.batch.transacciones;

import cl.duoc.bank.batch.common.RegistroInvalidoException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reglas de negocio del Job 1.
 *
 * Tres desenlaces posibles para una fila, y la diferencia importa:
 *
 *  1. Lanzar RegistroInvalidoException -> la fila se SALTA (dato defectuoso).
 *     Caso: 'tipo' fuera de {debito, credito}, o fecha nula.
 *
 *  2. Devolver null -> la fila se FILTRA (decision de negocio, no un error).
 *     Caso: monto <= 0. Una transaccion de monto cero o negativo no es una
 *     transaccion; no tiene sentido guardarla ni sumarla al resumen.
 *
 *  3. Devolver la entidad, eventualmente con anomalia=true -> se PERSISTE.
 *     Caso: duplicado o monto sobre el umbral. Aqui se marca en vez de
 *     descartar: el dato existe y hay que poder auditarlo, pero queda
 *     senalado para revision.
 *
 * El estado de deduplicacion vive en esta instancia, por eso el bean se
 * declara @StepScope: se crea una instancia nueva por ejecucion del Step y el
 * Set no se arrastra entre corridas.
 */
@Slf4j
public class TransaccionProcessor implements ItemProcessor<TransaccionCsv, Transaccion> {

    private static final Set<String> TIPOS_VALIDOS = Set.of("debito", "credito");

    /** Umbral sobre el cual un monto se considera sospechoso. Configurable. */
    private final BigDecimal umbralAnomalia;

    /**
     * clave fecha|monto|tipo -> id de la transaccion que la reclamo primero.
     *
     * Se guarda el duenio y no solo la clave porque el Step es fault-tolerant:
     * si alguna fila se salta, Spring Batch hace rollback del chunk y lo
     * REPROCESA fila por fila para aislar la defectuosa. Con un simple Set,
     * esa segunda pasada marcaria como duplicadas transacciones que solo se
     * estaban reprocesando.
     *
     * ConcurrentHashMap y no HashMap: en la semana 2 el Step es multihilo, o sea
     * tres 'Batch-Thread-N' llaman a process() a la vez sobre ESTA misma
     * instancia (@StepScope da una instancia por ejecucion del Step, no por
     * hilo). Un HashMap bajo escritura concurrente puede perder entradas o
     * corromper sus buckets, y putIfAbsent no seria atomico: dos hilos podrian
     * reclamar la misma clave y ninguna transaccion quedaria marcada como
     * duplicada. ConcurrentHashMap garantiza esa atomicidad.
     *
     * Consecuencia asumida del paralelismo: cual de dos filas identicas queda
     * como "primera ocurrencia" depende del orden en que los hilos lleguen. El
     * par se detecta siempre; cual de las dos se marca puede variar entre
     * corridas. Para el objetivo (senalar el duplicado para auditoria) da igual.
     */
    private final Map<String, Long> primeraOcurrencia = new ConcurrentHashMap<>();

    public TransaccionProcessor(BigDecimal umbralAnomalia) {
        this.umbralAnomalia = umbralAnomalia;
    }

    @Override
    public Transaccion process(TransaccionCsv csv) {

        // --- 1. Validaciones que descalifican la fila --------------------
        if (csv.getFecha() == null) {
            throw new RegistroInvalidoException("fecha nula en la transaccion id=" + csv.getId());
        }
        if (csv.getMonto() == null) {
            throw new RegistroInvalidoException("monto nulo en la transaccion id=" + csv.getId());
        }

        String tipo = csv.getTipo() == null ? "" : csv.getTipo().trim().toLowerCase();
        if (!TIPOS_VALIDOS.contains(tipo)) {
            throw new RegistroInvalidoException(
                    "tipo mal clasificado '" + csv.getTipo() + "' en la transaccion id=" + csv.getId()
                            + " (esperado: debito o credito)");
        }

        // --- 2. Filtro de negocio: monto no positivo ---------------------
        if (csv.getMonto().compareTo(BigDecimal.ZERO) <= 0) {
            log.warn("[FILTRADA] transaccion id={} descartada por monto no positivo ({})",
                    csv.getId(), csv.getMonto());
            return null;
        }

        // --- 3. Marcas de anomalia ---------------------------------------
        Transaccion t = new Transaccion();
        t.setId(csv.getId());
        t.setFecha(csv.getFecha());
        t.setMonto(csv.getMonto());
        t.setTipo(tipo);

        String clave = csv.getFecha() + "|" + csv.getMonto().stripTrailingZeros().toPlainString() + "|" + tipo;
        Long duenio = primeraOcurrencia.putIfAbsent(clave, csv.getId());
        boolean duplicada = duenio != null && !duenio.equals(csv.getId());

        boolean montoAlto = csv.getMonto().compareTo(umbralAnomalia) > 0;

        if (duplicada && montoAlto) {
            marcar(t, "posible duplicado de (" + clave + ") y monto sobre el umbral " + umbralAnomalia);
        } else if (duplicada) {
            marcar(t, "posible duplicado de (" + clave + ")");
        } else if (montoAlto) {
            marcar(t, "monto " + csv.getMonto() + " supera el umbral " + umbralAnomalia);
        }

        return t;
    }

    private void marcar(Transaccion t, String motivo) {
        t.setAnomalia(true);
        t.setMotivoAnomalia(motivo);
        log.warn("[ANOMALIA] transaccion id={} -> {}", t.getId(), motivo);
    }
}
