package cl.duoc.bank.batch.transacciones;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Fila cruda de transacciones.csv: id,fecha,monto,tipo
 *
 * Es un DTO deliberadamente separado de la entidad: lo que viene del archivo
 * todavia no es un dato confiable. Recien despues del ItemProcessor se
 * convierte en una {@link Transaccion} apta para persistir.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class TransaccionCsv {

    private Long id;
    private LocalDate fecha;
    private BigDecimal monto;
    private String tipo;
}
