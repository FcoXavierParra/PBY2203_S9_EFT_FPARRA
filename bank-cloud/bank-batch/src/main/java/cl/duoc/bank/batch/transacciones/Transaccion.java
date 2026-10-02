package cl.duoc.bank.batch.transacciones;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Transaccion diaria ya validada, tal como queda en la base.
 *
 * El id NO es autogenerado: viene del archivo de origen. En una migracion eso
 * es lo que se quiere, conservar la llave del sistema legacy para poder
 * conciliar despues.
 */
@Entity
@Table(name = "transaccion")
@Getter
@Setter
@NoArgsConstructor
@ToString
public class Transaccion {

    @Id
    @Column(name = "id")
    private Long id;

    @Column(name = "fecha", nullable = false)
    private LocalDate fecha;

    /** Siempre BigDecimal: con double los montos de dinero acumulan error. */
    @Column(name = "monto", nullable = false, precision = 19, scale = 2)
    private BigDecimal monto;

    @Column(name = "tipo", nullable = false, length = 20)
    private String tipo;

    /** true si el processor la marco como sospechosa (monto alto o duplicada). */
    @Column(name = "anomalia", nullable = false)
    private boolean anomalia;

    @Column(name = "motivo_anomalia", length = 200)
    private String motivoAnomalia;
}
