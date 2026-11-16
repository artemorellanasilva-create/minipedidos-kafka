
package cl.minipedidos.kafka.admin;

// Apache Kafka
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

// Spring Boot
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

// Java
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/admin/kafka")
public class KafkaAdminController {

    private final KafkaAdminService servicio;
    private final String tokenAdministrativo;

    // =========================================
    // 1. CONFIGURACION DEL CONTROLADOR
    // =========================================

    public KafkaAdminController(
            KafkaAdminService servicio,
            @Value("${MINIPEDIDOS_ADMIN_TOKEN:}")
            String tokenAdministrativo) {

        this.servicio = servicio;
        this.tokenAdministrativo = tokenAdministrativo;
    }

    // =========================================
    // 2. LISTAR TOPICOS
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

    @PostMapping("/topicos")
    public ResponseEntity<?> crearTopico(
            @RequestHeader(
                    value = "X-Admin-Token",
                    required = false
            )
            String tokenRecibido,

            @RequestBody
            CrearTopicoSolicitud solicitud) {

        // Verificar que exista un token configurado
        if (tokenAdministrativo == null
                || tokenAdministrativo.isBlank()) {

            return ResponseEntity.status(
                    HttpStatus.SERVICE_UNAVAILABLE
            ).body(Map.of(
                    "error",
                    "La administracion no esta configurada."
            ));
        }

        // Validar autorizacion administrativa
        if (!tokenValido(tokenRecibido)) {

            return ResponseEntity.status(
                    HttpStatus.FORBIDDEN
            ).body(Map.of(
                    "error",
                    "Acceso administrativo no autorizado."
            ));
        }

        // Validar datos recibidos
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

        if (solicitud.particiones() < 1
                || solicitud.particiones() > 20) {

            return ResponseEntity.badRequest().body(
                    Map.of(
                            "error",
                            "Las particiones deben estar entre 1 y 20."
                    )
            );
        }

        if (solicitud.replicas() != 1) {

            return ResponseEntity.badRequest().body(
                    Map.of(
                            "error",
                            "El laboratorio utiliza replicacion 1."
                    )
            );
        }

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
    // 7. VALIDAR TOKEN ADMINISTRATIVO
    // =========================================

    private boolean tokenValido(String tokenRecibido) {

        if (tokenRecibido == null) {
            return false;
        }

        byte[] esperado = tokenAdministrativo.getBytes(
                StandardCharsets.UTF_8
        );

        byte[] recibido = tokenRecibido.getBytes(
                StandardCharsets.UTF_8
        );

        return MessageDigest.isEqual(
                esperado,
                recibido
        );
    }

    // =========================================
    // 8. MANEJO DE ERRORES KAFKA
    // =========================================

    private ResponseEntity<?> manejarError(Exception e) {

        Throwable causa = e;

        while (causa instanceof ExecutionException
                && causa.getCause() != null) {

            causa = causa.getCause();
        }

        if (causa instanceof UnknownTopicOrPartitionException) {

            return ResponseEntity.status(
                    HttpStatus.NOT_FOUND
            ).body(Map.of(
                    "error",
                    "El topico no existe."
            ));
        }

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
    // 9. ERROR DE SERVICIO
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
