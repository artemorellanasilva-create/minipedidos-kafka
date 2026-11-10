
package cl.minipedidos.kafka.admin;

import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/admin/kafka")
public class KafkaAdminController {

    private final KafkaAdminService servicio;

    public KafkaAdminController(KafkaAdminService servicio) {
        this.servicio = servicio;
    }

    // =========================================
    // 1. LISTAR TOPICOS
    // =========================================

    @GetMapping("/topicos")
    public ResponseEntity<?> listarTopicos() {

        try {
            List<String> topicos = servicio.listarTopicos();

            return ResponseEntity.ok(topicos);

        } catch (Exception e) {
            return errorServicio();
        }
    }

    // =========================================
    // 2. CONSULTAR PARTICIONES Y REPLICAS
    // =========================================

    @GetMapping("/topicos/{nombre}")
    public ResponseEntity<?> consultarTopico(
            @PathVariable String nombre) {

        try {
            return ResponseEntity.ok(
                    servicio.consultarTopico(nombre)
            );

        } catch (Exception e) {
            return manejarError(e);
        }
    }

    // =========================================
    // 3. CONSULTAR CONFIGURACIONES DEL TOPICO
    // =========================================

    @GetMapping("/topicos/{nombre}/configuracion")
    public ResponseEntity<?> consultarConfiguracion(
            @PathVariable String nombre) {

        try {
            Map<String, String> configuracion =
                    servicio.consultarConfiguracion(nombre);

            return ResponseEntity.ok(configuracion);

        } catch (Exception e) {
            return manejarError(e);
        }
    }

    // =========================================
    // 4. MANEJO DE ERRORES
    // =========================================

    private ResponseEntity<?> manejarError(Exception e) {

        Throwable causa = e;

        while (causa instanceof ExecutionException
                && causa.getCause() != null) {
            causa = causa.getCause();
        }

        if (causa instanceof UnknownTopicOrPartitionException) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of(
                            "error", "El topico no existe"
                    ));
        }

        return errorServicio();
    }

    private ResponseEntity<?> errorServicio() {

        return ResponseEntity.status(
                HttpStatus.SERVICE_UNAVAILABLE
        ).body(Map.of(
                "error", "No fue posible consultar Kafka"
        ));
    }
}
