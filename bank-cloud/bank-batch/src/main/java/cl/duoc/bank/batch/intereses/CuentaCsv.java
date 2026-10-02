package cl.duoc.bank.batch.intereses;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;

/**
 * Fila cruda de intereses.csv: cuenta_id,nombre,saldo,edad,tipo
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class CuentaCsv {

    private Long cuentaId;
    private String nombre;
    private BigDecimal saldo;
    private Integer edad;
    private String tipo;
}
