
package cl.minipedidos.kafka.admin;

// ==========================================
// MINIPEDIDOS V2 - SESION 3 - PARTE 3
// ADMINISTRACION DE TOPICOS KAFKA
// AUTORIZACION MEDIANTE SPRING SECURITY
// ==========================================

// Apache Kafka
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

// Spring Boot
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

// Java
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/admin/kafka")
public class KafkaAdminController {

    private final KafkaAdminService servicio;

    // =========================================
    // 1. CONFIGURACION DEL CONTROLADOR
    // =========================================

    public KafkaAdminController(
            KafkaAdminService servicio) {

        this.servicio = servicio;
    }

    // =========================================
    // 2. LISTAR TOPICOS
    // =========================================

    @GetMapping("/topicos")
    public ResponseEntity<?> listarTopicos() {

        try {

            List<String> topicos =
                    servicio.listarTopicos();

            return ResponseEntity.ok(topicos);

        } catch (Exception e) {

            return errorServicio();
        }
    }

    // =========================================
    // 3. CONSULTAR PARTICIONES Y REPLICAS
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
    // 4. CONSULTAR CONFIGURACIONES
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
    // 5. DATOS PARA CREAR UN TOPICO
    // =========================================

    public record CrearTopicoSolicitud(
            String nombre,
            Integer particiones,
            Integer replicas
    ) {}

    // =========================================
    // 6. CREAR UN NUEVO TOPICO
    // =========================================

    /*
     * La autorizacion se realiza mediante
     * Spring Security.
     *
     * POST /api/admin/kafka/topicos
     * Requiere ROLE_ADMIN.
     *
     * Spring Security tambien verifica CSRF.
     */

    @PostMapping("/topicos")
    public ResponseEntity<?> crearTopico(
            @RequestBody
            CrearTopicoSolicitud solicitud) {

        // =====================================
        // 6.1. VALIDAR DATOS RECIBIDOS
        // =====================================

        if (solicitud == null
                || solicitud.nombre() == null
                || solicitud.particiones() == null
                || solicitud.replicas() == null) {

            return ResponseEntity.badRequest().body(
                    Map.of(
                            "error",
                            "Debe indicar nombre, particiones y replicas."
                    )
            );
        }

        // =====================================
        // 6.2. VALIDAR CANTIDAD DE PARTICIONES
        // =====================================

        if (solicitud.particiones() < 1
                || solicitud.particiones() > 20) {

            return ResponseEntity.badRequest().body(
                    Map.of(
                            "error",
                            "Las particiones deben estar entre 1 y 20."
                    )
            );
        }

        // =====================================
        // 6.3. VALIDAR FACTOR DE REPLICACION
        // =====================================

        if (solicitud.replicas() != 1) {

            return ResponseEntity.badRequest().body(
                    Map.of(
                            "error",
                            "El laboratorio utiliza replicacion 1."
                    )
            );
        }

        // =====================================
        // 6.4. CREAR TOPICO EN APACHE KAFKA
        // =====================================

        try {

            Map<String, Object> resultado =
                    servicio.crearTopico(
                            solicitud.nombre(),
                            solicitud.particiones(),
                            solicitud.replicas().shortValue()
                    );

            return ResponseEntity.status(
                    HttpStatus.CREATED
            ).body(resultado);

        } catch (IllegalArgumentException e) {

            return ResponseEntity.badRequest().body(
                    Map.of(
                            "error",
                            e.getMessage()
                    )
            );

        } catch (Exception e) {

            return manejarError(e);
        }
    }

    // =========================================
    // 7. MANEJO DE ERRORES DE KAFKA
    // =========================================

    private ResponseEntity<?> manejarError(Exception e) {

        Throwable causa = e;

        while (causa instanceof ExecutionException
                && causa.getCause() != null) {

            causa = causa.getCause();
        }

        // Topico inexistente
        if (causa instanceof UnknownTopicOrPartitionException) {

            return ResponseEntity.status(
                    HttpStatus.NOT_FOUND
            ).body(Map.of(
                    "error",
                    "El topico no existe."
            ));
        }

        // Topico duplicado
        if (causa instanceof TopicExistsException) {

            return ResponseEntity.status(
                    HttpStatus.CONFLICT
            ).body(Map.of(
                    "error",
                    "El topico ya existe."
            ));
        }

        return errorServicio();
    }

    // =========================================
    // 8. ERROR DE SERVICIO
    // =========================================

    private ResponseEntity<?> errorServicio() {

        return ResponseEntity.status(
                HttpStatus.SERVICE_UNAVAILABLE
        ).body(Map.of(
                "error",
                "No fue posible completar la operacion en Kafka."
        ));
    }
}
