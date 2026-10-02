package cl.duoc.bank.batch.anuales;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Fila cruda de cuentas_anuales.csv:
 * cuenta_id,fecha,transaccion,monto,descripcion
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class MovimientoAnualCsv {

    private Long cuentaId;
    private LocalDate fecha;
    private String transaccion;
    private BigDecimal monto;
    private String descripcion;
}
