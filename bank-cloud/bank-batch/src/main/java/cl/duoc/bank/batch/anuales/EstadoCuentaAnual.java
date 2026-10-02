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

/**
 * Estado de cuenta anual: una fila por cuenta y ano.
 *
 * Los totales de retiros y compras quedan con signo negativo, igual que en el
 * origen, de modo que saldoNeto es literalmente la suma de los tres.
 */
@Entity
@Table(name = "estado_cuenta_anual")
@Getter
@Setter
@NoArgsConstructor
@ToString
public class EstadoCuentaAnual {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "cuenta_id", nullable = false)
    private Long cuentaId;

    @Column(name = "anio", nullable = false)
    private int anio;

    @Column(name = "total_depositos", nullable = false, precision = 19, scale = 2)
    private BigDecimal totalDepositos = BigDecimal.ZERO;

    /** Negativo, tal como viene del origen. */
    @Column(name = "total_retiros", nullable = false, precision = 19, scale = 2)
    private BigDecimal totalRetiros = BigDecimal.ZERO;

    /** Negativo, tal como viene del origen. */
    @Column(name = "total_compras", nullable = false, precision = 19, scale = 2)
    private BigDecimal totalCompras = BigDecimal.ZERO;

    /** depositos + retiros + compras. */
    @Column(name = "saldo_neto", nullable = false, precision = 19, scale = 2)
    private BigDecimal saldoNeto = BigDecimal.ZERO;

    @Column(name = "cantidad_movimientos", nullable = false)
    private int cantidadMovimientos;

    @Column(name = "movimientos_en_cero", nullable = false)
    private int movimientosEnCero;
}
