package cl.duoc.bank.batch.common;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Parametros de escalado, en un solo lugar.
 *
 * Todo lo que el benchmark necesita mover se sobreescribe por linea de comandos
 * sin recompilar:
 *
 *   --bank.escalado.modo=particiones --bank.escalado.grid-size=4
 *   --bank.escalado.modo=multihilo   --bank.batch.hilos=3
 *
 * grid-size es la cantidad de particiones, NO la cantidad de hilos. Son cosas
 * distintas y conviene no confundirlas: las particiones se reparten sobre el
 * pool de particionTaskExecutor, asi que con grid-size 8 y un pool de 4 se
 * procesan 8 rangos de a 4 en paralelo. Dejar grid-size por encima del pool es
 * util cuando las particiones son desparejas, porque un hilo que termina
 * temprano toma la siguiente en vez de quedarse ocioso.
 */
@Slf4j
@Getter
@Component
public class EscaladoConfig {

    private final ModoEscalado modo;
    private final int gridSize;
    private final int hilosParticion;

    public EscaladoConfig(
            @Value("${bank.escalado.modo:multihilo}") String modo,
            @Value("${bank.escalado.grid-size:3}") int gridSize,
            @Value("${bank.escalado.hilos-particion:3}") int hilosParticion) {

        this.modo = ModoEscalado.valueOf(modo.trim().toUpperCase());
        this.gridSize = Math.max(1, gridSize);
        this.hilosParticion = Math.max(1, hilosParticion);

        log.info("Modo de escalado: {} (grid-size={}, hilos de particion={})",
                this.modo, this.gridSize, this.hilosParticion);
    }

    public boolean esParticionado() {
        return modo == ModoEscalado.PARTICIONES;
    }
}
