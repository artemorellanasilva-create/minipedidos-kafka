
package cl.minipedidos.kafka.admin;

// ==========================================
// MINIPEDIDOS V2 - SESION 3 - PARTE 3
// ADMINISTRACION DE TOPICOS KAFKA
// SPRING SECURITY Y CSRF
// PASO 5.18 - MODIFICACION DE RETENCION
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

    // ==========================================
    // 1. CONFIGURACION DEL CONTROLADOR
    // ==========================================

    public KafkaAdminController(
            KafkaAdminService servicio) {

        this.servicio = servicio;
    }

    // ==========================================
    // 2. LISTAR TOPICOS
    // ==========================================

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

    // ==========================================
    // 3. CONSULTAR PARTICIONES Y REPLICAS
    // ==========================================

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

    // ==========================================
    // 4. CONSULTAR CONFIGURACIONES
    // ==========================================

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

    // ==========================================
    // 5. DATOS PARA CREAR UN TOPICO
    // ==========================================

    public record CrearTopicoSolicitud(
            String nombre,
            Integer particiones,
            Integer replicas
    ) {}

    // ==========================================
    // 6. CREAR UN NUEVO TOPICO
    // ==========================================

    /*
     * POST /api/admin/kafka/topicos
     *
     * Spring Security:
     * - Requiere ROLE_ADMIN.
     * - Verifica token CSRF.
     *
     * No utiliza X-Admin-Token.
     */

    @PostMapping("/topicos")
    public ResponseEntity<?> crearTopico(
            @RequestBody CrearTopicoSolicitud solicitud) {

        // --------------------------------------
        // 6.1. VALIDAR DATOS RECIBIDOS
        // --------------------------------------

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

        // --------------------------------------
        // 6.2. VALIDAR PARTICIONES
        // --------------------------------------

        if (solicitud.particiones() < 1
                || solicitud.particiones() > 20) {

            return ResponseEntity.badRequest().body(
                    Map.of(
                            "error",
                            "Las particiones deben estar entre 1 y 20."
                    )
            );
        }

        // --------------------------------------
        // 6.3. VALIDAR REPLICAS
        // --------------------------------------

        if (solicitud.replicas() != 1) {

            return ResponseEntity.badRequest().body(
                    Map.of(
                            "error",
                            "El laboratorio utiliza replicacion 1."
                    )
            );
        }

        // --------------------------------------
        // 6.4. CREAR TOPICO EN KAFKA
        // --------------------------------------

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

    // ==========================================
    // 7. DATOS PARA MODIFICAR RETENCION
    // ==========================================

    /*
     * El dashboard enviara un JSON como:
     *
     * {
     *     "dias": 3
     * }
     */

    public record ModificarRetencionSolicitud(
            Integer dias
    ) {}

    // ==========================================
    // 8. MODIFICAR RETENCION DE UN TOPICO
    // ==========================================

    /*
     * PUT /api/admin/kafka/topicos/{nombre}/retencion
     *
     * Spring Security:
     * - Requiere ROLE_ADMIN.
     * - Verifica token CSRF.
     *
     * El servicio Kafka modificara retention.ms.
     */

    @PutMapping("/topicos/{nombre}/retencion")
    public ResponseEntity<?> modificarRetencion(
            @PathVariable String nombre,
            @RequestBody ModificarRetencionSolicitud solicitud) {

        // --------------------------------------
        // 8.1. VALIDAR SOLICITUD
        // --------------------------------------

        if (solicitud == null
                || solicitud.dias() == null) {

            return ResponseEntity.badRequest().body(
                    Map.of(
                            "error",
                            "Debe indicar los dias de retencion."
                    )
            );
        }

        // --------------------------------------
        // 8.2. VALIDAR RANGO PERMITIDO
        // --------------------------------------

        if (solicitud.dias() < 1
                || solicitud.dias() > 30) {

            return ResponseEntity.badRequest().body(
                    Map.of(
                            "error",
                            "La retencion debe estar entre 1 y 30 dias."
                    )
            );
        }

        // --------------------------------------
        // 8.3. MODIFICAR CONFIGURACION KAFKA
        // --------------------------------------

        try {

            Map<String, Object> resultado =
                    servicio.modificarRetencion(
                            nombre,
                            solicitud.dias()
                    );

            return ResponseEntity.ok(resultado);

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

    // ==========================================
    // 9. MANEJO DE ERRORES DE KAFKA
    // ==========================================

    private ResponseEntity<?> manejarError(Exception e) {

        Throwable causa = e;

        while (causa instanceof ExecutionException
                && causa.getCause() != null) {

            causa = causa.getCause();
        }

        // --------------------------------------
        // 9.1. TOPICO INEXISTENTE
        // --------------------------------------

        if (causa instanceof UnknownTopicOrPartitionException) {

            return ResponseEntity.status(
                    HttpStatus.NOT_FOUND
            ).body(Map.of(
                    "error",
                    "El topico no existe."
            ));
        }

        // --------------------------------------
        // 9.2. TOPICO DUPLICADO
        // --------------------------------------

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

    // ==========================================
    // 10. ERROR GENERAL DEL SERVICIO KAFKA
    // ==========================================

    private ResponseEntity<?> errorServicio() {

        return ResponseEntity.status(
                HttpStatus.SERVICE_UNAVAILABLE
        ).body(Map.of(
                "error",
                "No fue posible completar la operacion en Kafka."
        ));
    }
}
