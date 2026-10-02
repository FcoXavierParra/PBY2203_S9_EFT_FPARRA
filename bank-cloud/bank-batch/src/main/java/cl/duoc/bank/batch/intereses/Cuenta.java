package cl.duoc.bank.batch.intereses;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;

/**
 * Cuenta con su interes mensual ya aplicado.
 *
 * Se guarda el saldo inicial ademas del final a proposito: sin el saldo de
 * partida no hay forma de auditar despues si el interes se calculo bien.
 */
@Entity
@Table(name = "cuenta")
@Getter
@Setter
@NoArgsConstructor
@ToString
public class Cuenta {

    @Id
    @Column(name = "cuenta_id")
    private Long cuentaId;

    @Column(name = "nombre", nullable = false, length = 120)
    private String nombre;

    @Column(name = "saldo_inicial", nullable = false, precision = 19, scale = 2)
    private BigDecimal saldoInicial;

    @Column(name = "edad")
    private Integer edad;

    @Column(name = "tipo", nullable = false, length = 20)
    private String tipo;

    /** Tasa mensual efectivamente aplicada, para poder reconstruir el calculo. */
    @Column(name = "tasa_aplicada", precision = 9, scale = 6)
    private BigDecimal tasaAplicada;

    @Column(name = "interes_calculado", precision = 19, scale = 2)
    private BigDecimal interesCalculado;

    @Column(name = "saldo_final", nullable = false, precision = 19, scale = 2)
    private BigDecimal saldoFinal;

    /** Notas del processor: duplicado sospechoso, saldo cero, edad limite. */
    @Column(name = "observacion", length = 250)
    private String observacion;
}
