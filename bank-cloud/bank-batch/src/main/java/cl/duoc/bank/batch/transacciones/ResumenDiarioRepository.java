package cl.duoc.bank.batch.transacciones;

import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDate;
import java.util.Optional;

public interface ResumenDiarioRepository extends JpaRepository<ResumenDiario, Long> {

    Optional<ResumenDiario> findByFecha(LocalDate fecha);
}
