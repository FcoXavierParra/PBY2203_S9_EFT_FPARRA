package cl.duoc.bank.batch.common;

import org.springframework.batch.item.file.transform.FieldSet;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Lectura de campos del CSV con mensajes de error utilizables.
 *
 * Existe por una razon concreta que aparecio al correr el dataset de la semana
 * 2. La fila '4,2024-01-03,,debito' tiene el monto vacio, y hacer
 * new BigDecimal("") directamente producia esto en el archivo de auditoria:
 *
 *   ArrayIndexOutOfBoundsException: Index 0 out of bounds for length 0
 *
 * Tecnicamente correcto e inservible: quien revise errores.csv no tiene forma de
 * saber que el problema era un monto vacio. Con estos metodos la misma fila
 * queda registrada como:
 *
 *   RegistroInvalidoException: el campo 'monto' viene vacio
 *
 * Las excepciones se lanzan dentro del fieldSetMapper, asi que Spring Batch las
 * envuelve en FlatFileParseException y BankSkipPolicy las trata como dato sucio
 * igual que antes. Cambia el mensaje, no el comportamiento.
 */
public final class CamposCsv {

    private CamposCsv() {
        // Clase de utilidad.
    }

    /** Devuelve el campo sin espacios, o lanza si viene vacio o ausente. */
    public static String obligatorio(FieldSet fs, String campo) {
        String valor = fs.readString(campo);
        if (valor == null || valor.isBlank()) {
            throw new RegistroInvalidoException("el campo '" + campo + "' viene vacio");
        }
        return valor.trim();
    }

    /** Devuelve el campo sin espacios, o null si viene vacio. */
    public static String opcional(FieldSet fs, String campo) {
        String valor = fs.readString(campo);
        return (valor == null || valor.isBlank()) ? null : valor.trim();
    }

    public static BigDecimal decimal(FieldSet fs, String campo) {
        String valor = obligatorio(fs, campo);
        try {
            return new BigDecimal(valor);
        } catch (NumberFormatException | ArithmeticException e) {
            throw new RegistroInvalidoException(
                    "el campo '" + campo + "' no es un numero valido: '" + valor + "'");
        }
    }

    public static Integer entero(FieldSet fs, String campo) {
        String valor = obligatorio(fs, campo);
        try {
            return Integer.valueOf(valor);
        } catch (NumberFormatException e) {
            throw new RegistroInvalidoException(
                    "el campo '" + campo + "' no es un entero valido: '" + valor + "'");
        }
    }

    public static Long enteroLargo(FieldSet fs, String campo) {
        String valor = obligatorio(fs, campo);
        try {
            return Long.valueOf(valor);
        } catch (NumberFormatException e) {
            throw new RegistroInvalidoException(
                    "el campo '" + campo + "' no es un entero valido: '" + valor + "'");
        }
    }

    /**
     * Normaliza la fecha a LocalDate aceptando los cuatro formatos que aparecen
     * en los archivos del sistema legacy.
     *
     * Por que se normaliza en vez de descartar
     * ----------------------------------------
     * El enunciado pide "corregir o manejar los errores en los datos" y
     * "manejar reglas para asegurar su consistencia". Los numeros del dataset
     * semana_3 (1000 filas) obligan a tomarse en serio esa palabra:
     *
     *   yyyy-MM-dd (ISO, valida) ....... 239
     *   yyyy/MM/dd ..................... 222
     *   dd-MM-yyyy ..................... 250
     *   dd/MM/yyyy ..................... 234
     *   ISO con mes > 12 (2024-13-01) ...  55
     *
     * Aceptar solo ISO descartaria 761 de 1000 filas, un 76 %, por un separador.
     * Eso no es rigor, es perder los datos del banco.
     *
     * SUPUESTO EXPLICITO, y es el punto discutible de esta clase: en los
     * formatos dd-MM-yyyy y dd/MM/yyyy se asume DIA PRIMERO. Cuando ambos
     * numeros son <= 12 (por ejemplo 04/05/2024) el valor es genuinamente
     * ambiguo y no hay forma de resolverlo desde el archivo. Se elige dia
     * primero por dos razones: es la convencion en Chile, y el propio dataset lo
     * respalda con casos que solo se explican asi (17-06-2024, 24/03/2024,
     * 30-07-2024 tienen un primer campo > 12). Si el sistema de origen resultara
     * ser mes-primero, hay que cambiar FORMATOS_DIA_PRIMERO y volver a cargar.
     *
     * Lo que NO se corrige: una fecha con mes 13 o dia 32 no es un problema de
     * formato sino un valor imposible. Esas filas se descartan y quedan en
     * errores.csv, porque inventarles un valor si seria falsear el dato.
     */
    public static LocalDate fecha(FieldSet fs, String campo) {
        String valor = obligatorio(fs, campo);

        // 1. ISO, el formato correcto. LocalDate.parse ya rechaza mes 13 o dia 32.
        try {
            return LocalDate.parse(valor);
        } catch (DateTimeParseException ignorada) {
            // Sigue con los formatos recuperables.
        }

        // 2. Formatos con anio primero: el separador cambia, el orden no. No hay
        //    ambiguedad posible.
        for (DateTimeFormatter formato : FORMATOS_ANIO_PRIMERO) {
            LocalDate fecha = intentar(valor, formato);
            if (fecha != null) {
                return fecha;
            }
        }

        // 3. Formatos con dia primero. Aqui aplica el supuesto documentado arriba.
        for (DateTimeFormatter formato : FORMATOS_DIA_PRIMERO) {
            LocalDate fecha = intentar(valor, formato);
            if (fecha != null) {
                return fecha;
            }
        }

        throw new RegistroInvalidoException(
                "el campo '" + campo + "' no es una fecha reconocible ni corregible: '" + valor
                        + "' (formatos aceptados: yyyy-MM-dd, yyyy/MM/dd, dd-MM-yyyy, dd/MM/yyyy)");
    }

    private static final DateTimeFormatter[] FORMATOS_ANIO_PRIMERO = {
            DateTimeFormatter.ofPattern("yyyy/MM/dd"),
    };

    private static final DateTimeFormatter[] FORMATOS_DIA_PRIMERO = {
            DateTimeFormatter.ofPattern("dd-MM-yyyy"),
            DateTimeFormatter.ofPattern("dd/MM/yyyy"),
    };

    /**
     * Devuelve la fecha o null si el patron no calza.
     *
     * ResolverStyle.STRICT no sirve aqui porque exigiria el patron 'uuuu' en vez
     * de 'yyyy'; con el estilo por defecto (SMART) un dia 32 se rechaza igual,
     * que es lo que interesa. Lo que SMART si haria es convertir el 31 de
     * febrero en 28 de febrero, pero eso requiere que el patron calce primero, y
     * una fecha asi no aparece en estos archivos.
     */
    private static LocalDate intentar(String valor, DateTimeFormatter formato) {
        try {
            return LocalDate.parse(valor, formato);
        } catch (DateTimeParseException e) {
            return null;
        }
    }
}
