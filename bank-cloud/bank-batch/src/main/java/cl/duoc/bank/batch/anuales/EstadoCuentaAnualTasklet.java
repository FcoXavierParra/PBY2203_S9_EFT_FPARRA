package cl.duoc.bank.batch.anuales;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Ultimo Step del Job 3: agrupa los movimientos por cuenta y ano, escribe la
 * tabla estado_cuenta_anual y ademas genera el informe en archivo de texto que
 * pide el enunciado para auditorias.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EstadoCuentaAnualTasklet implements Tasklet {

    private final MovimientoAnualRepository movimientoRepository;
    private final EstadoCuentaAnualRepository estadoRepository;

    @Value("${bank.anuales.archivo-informe:reportes/estados_cuenta_anuales.txt}")
    private String rutaInforme;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws IOException {

        List<MovimientoAnual> movimientos = movimientoRepository.findAll();
        log.info("Compilando estados de cuenta anuales sobre {} movimientos", movimientos.size());

        // Clave "cuentaId|anio" en TreeMap para que salga ordenado.
        Map<String, EstadoCuentaAnual> estados = new TreeMap<>();

        for (MovimientoAnual m : movimientos) {
            int anio = m.getFecha().getYear();
            String clave = m.getCuentaId() + "|" + anio;

            EstadoCuentaAnual e = estados.computeIfAbsent(clave, k -> {
                EstadoCuentaAnual nuevo = new EstadoCuentaAnual();
                nuevo.setCuentaId(m.getCuentaId());
                nuevo.setAnio(anio);
                return nuevo;
            });

            switch (m.getTipoTransaccion()) {
                case "deposito" -> e.setTotalDepositos(e.getTotalDepositos().add(m.getMonto()));
                case "retiro"   -> e.setTotalRetiros(e.getTotalRetiros().add(m.getMonto()));
                case "compra"   -> e.setTotalCompras(e.getTotalCompras().add(m.getMonto()));
                default -> log.error("Tipo inesperado '{}' al agregar; no deberia llegar aqui",
                        m.getTipoTransaccion());
            }

            e.setSaldoNeto(e.getSaldoNeto().add(m.getMonto()));
            e.setCantidadMovimientos(e.getCantidadMovimientos() + 1);
            if (m.isMontoCero()) {
                e.setMovimientosEnCero(e.getMovimientosEnCero() + 1);
            }
        }

        estadoRepository.saveAll(estados.values());
        escribirInforme(estados.values());

        log.info("---------------- ESTADOS DE CUENTA ANUALES ----------------");
        estados.values().forEach(e ->
                log.info("cuenta {} ({}) | depositos={} | retiros={} | compras={} | neto={} | movs={} | en cero={}",
                        e.getCuentaId(), e.getAnio(), e.getTotalDepositos(), e.getTotalRetiros(),
                        e.getTotalCompras(), e.getSaldoNeto(), e.getCantidadMovimientos(),
                        e.getMovimientosEnCero()));
        log.info("Se escribieron {} estados de cuenta", estados.size());
        log.info("----------------------------------------------------------");

        contribution.incrementWriteCount(estados.size());
        return RepeatStatus.FINISHED;
    }

    /** Informe de texto plano, en UTF-8 por las descripciones con tildes y enie. */
    private void escribirInforme(Iterable<EstadoCuentaAnual> estados) throws IOException {
        Path destino = Path.of(rutaInforme);
        if (destino.getParent() != null) {
            Files.createDirectories(destino.getParent());
        }

        StringBuilder sb = new StringBuilder();
        sb.append("INFORME DE ESTADOS DE CUENTA ANUALES - Banco XYZ\n");
        sb.append("=".repeat(110)).append('\n');
        sb.append(String.format("%-10s %-6s %14s %14s %14s %14s %8s %10s%n",
                "CUENTA", "ANIO", "DEPOSITOS", "RETIROS", "COMPRAS", "SALDO NETO", "MOVS", "EN CERO"));
        sb.append("-".repeat(110)).append('\n');

        BigDecimal netoGlobal = BigDecimal.ZERO;
        int totalMovs = 0;

        for (EstadoCuentaAnual e : estados) {
            sb.append(String.format("%-10d %-6d %14s %14s %14s %14s %8d %10d%n",
                    e.getCuentaId(), e.getAnio(), e.getTotalDepositos(), e.getTotalRetiros(),
                    e.getTotalCompras(), e.getSaldoNeto(), e.getCantidadMovimientos(),
                    e.getMovimientosEnCero()));
            netoGlobal = netoGlobal.add(e.getSaldoNeto());
            totalMovs += e.getCantidadMovimientos();
        }

        sb.append("-".repeat(110)).append('\n');
        sb.append(String.format("TOTAL GENERAL -> saldo neto %s sobre %d movimientos%n", netoGlobal, totalMovs));

        Files.writeString(destino, sb.toString(), StandardCharsets.UTF_8);
        log.info("Informe escrito en {}", destino.toAbsolutePath());
    }
}
