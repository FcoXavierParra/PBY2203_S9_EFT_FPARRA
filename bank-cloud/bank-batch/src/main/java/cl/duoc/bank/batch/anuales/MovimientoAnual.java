package cl.duoc.bank.batch.anuales;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Movimiento individual del ano, ya validado.
 *
 * El monto conserva el signo del archivo de origen: los retiros y las compras
 * vienen negativos y asi se guardan. Normalizarlos a positivo obligaria a
 * recordar el signo en cada consulta posterior.
 */
@Entity
@Table(name = "movimiento_anual")
@Getter
@Setter
@NoArgsConstructor
@ToString
public class MovimientoAnual {

    /** El CSV no trae identificador propio, asi que la base lo genera. */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "cuenta_id", nullable = false)
    private Long cuentaId;

    @Column(name = "fecha", nullable = false)
    private LocalDate fecha;

    @Column(name = "tipo_transaccion", nullable = false, length = 20)
    private String tipoTransaccion;

    @Column(name = "monto", nullable = false, precision = 19, scale = 2)
    private BigDecimal monto;

    @Column(name = "descripcion", length = 250)
    private String descripcion;

    /** Movimiento de monto 0: valido pero sin efecto, se marca para auditoria. */
    @Column(name = "monto_cero", nullable = false)
    private boolean montoCero;

    @Column(name = "observacion", length = 250)
    private String observacion;
}
