
package cl.minipedidos.kafka.security;

// ==========================================
// MINIPEDIDOS V2 - SESION 3 - PARTE 3
// TOKEN CSRF PARA EL DASHBOARD
// ==========================================

import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;

import org.springframework.security.web.csrf.CsrfToken;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class CsrfController {

    // ==========================================
    // 1. OBTENER CSRF DE LA SESION ACTUAL
    // ==========================================

    @GetMapping("/admin/csrf")
    public ResponseEntity<Map<String, String>> obtenerCsrf(
            CsrfToken csrfToken) {

        Map<String, String> respuesta = Map.of(
                "headerName", csrfToken.getHeaderName(),
                "parameterName", csrfToken.getParameterName(),
                "token", csrfToken.getToken()
        );

        return ResponseEntity.ok()
                .cacheControl(CacheControl.noStore())
                .body(respuesta);
    }
}
