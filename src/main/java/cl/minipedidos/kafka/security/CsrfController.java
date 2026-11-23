
package cl.minipedidos.kafka.security;

// ==========================================
// MINIPEDIDOS V2 - SESION 3 - PARTE 3
// PASO 5.17 - CSRF Y USUARIO AUTENTICADO
// ==========================================

import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.web.csrf.CsrfToken;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class CsrfController {

    // ==========================================
    // 1. OBTENER TOKEN CSRF
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

    // ==========================================
    // 2. OBTENER USUARIO CONECTADO Y ROLES
    // ==========================================

    @GetMapping("/admin/usuario")
    public ResponseEntity<Map<String, Object>> obtenerUsuario(
            Authentication authentication) {

        String nombreUsuario = authentication.getName();

        List<String> roles = authentication
                .getAuthorities()
                .stream()
                .map(GrantedAuthority::getAuthority)
                .filter(rol -> rol.startsWith("ROLE_"))
                .map(rol -> rol.substring(5))
                .sorted()
                .toList();

        Map<String, Object> respuesta = Map.of(
                "usuario", nombreUsuario,
                "roles", roles
        );

        return ResponseEntity.ok()
                .cacheControl(CacheControl.noStore())
                .body(respuesta);
    }
}
