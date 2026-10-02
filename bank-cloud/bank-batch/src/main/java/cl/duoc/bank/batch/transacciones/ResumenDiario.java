package cl.duoc.bank.batch.transacciones;

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
 * Salida agregada del Job 1: una fila por fecha con los totales del dia.
 * La produce el segundo Step, ya con las transacciones validas en la base.
 */
@Entity
@Table(name = "resumen_diario")
@Getter
@Setter
@NoArgsConstructor
@ToString
public class ResumenDiario {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "fecha", nullable = false, unique = true)
    private LocalDate fecha;

    @Column(name = "total_debitos", nullable = false, precision = 19, scale = 2)
    private BigDecimal totalDebitos = BigDecimal.ZERO;

    @Column(name = "total_creditos", nullable = false, precision = 19, scale = 2)
    private BigDecimal totalCreditos = BigDecimal.ZERO;

    @Column(name = "cantidad_transacciones", nullable = false)
    private int cantidadTransacciones;

    @Column(name = "cantidad_anomalias", nullable = false)
    private int cantidadAnomalias;
}
