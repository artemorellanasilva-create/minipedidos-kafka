
package cl.minipedidos.kafka.security;

// ==========================================
// MINIPEDIDOS V2 - SESION 3 - PARTE 3
// RECUPERACION DEL ADMINISTRADOR
// ==========================================

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.JdbcUserDetailsManager;
import org.springframework.security.web.csrf.CsrfToken;

import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

@Controller
public class AdminRecoveryController {

    private final JdbcUserDetailsManager usuarios;
    private final PasswordEncoder encoder;
    private final String claveRecuperacion;

    public AdminRecoveryController(
            JdbcUserDetailsManager usuarios,
            PasswordEncoder encoder,
            @Value("${MINIPEDIDOS_RECOVERY_KEY:}")
            String claveRecuperacion) {

        this.usuarios = usuarios;
        this.encoder = encoder;
        this.claveRecuperacion = claveRecuperacion;
    }

    // ==========================================
    // 1. MOSTRAR FORMULARIO DE RECUPERACION
    // ==========================================

    @GetMapping("/recovery/admin")
    public ResponseEntity<String> formulario(
            HttpServletRequest request) {

        if (!recuperacionHabilitada()) {
            return pagina(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "La recuperacion no esta habilitada."
            );
        }

        if (!usuarios.userExists("admin")) {
            return pagina(
                    HttpStatus.NOT_FOUND,
                    "La cuenta administradora no existe."
            );
        }

        CsrfToken csrf = (CsrfToken)
                request.getAttribute(
                        CsrfToken.class.getName()
                );

        if (csrf == null) {
            return pagina(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    "No fue posible preparar el formulario."
            );
        }

        String html = """
            <!DOCTYPE html>
            <html lang="es">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport"
                      content="width=device-width, initial-scale=1">

                <title>MiniPedidos V2 | Recuperar acceso</title>

                <style>
                    body {
                        font-family: Arial, sans-serif;
                        background: #f5f7fb;
                        color: #253247;
                        margin: 0;
                        padding: 24px;
                    }

                    main {
                        max-width: 460px;
                        margin: 50px auto;
                        padding: 30px;
                        background: white;
                        border: 1px solid #d8e1eb;
                        border-radius: 10px;
                    }

                    h1, h2 {
                        color: #183b63;
                    }

                    label {
                        display: block;
                        margin-top: 18px;
                        font-weight: bold;
                    }

                    input {
                        width: 100%%;
                        padding: 11px;
                        margin-top: 6px;
                        box-sizing: border-box;
                        border: 1px solid #b8c7d7;
                        border-radius: 6px;
                    }

                    button {
                        width: 100%%;
                        margin-top: 24px;
                        padding: 12px;
                        background: #183b63;
                        color: white;
                        border: none;
                        border-radius: 6px;
                        cursor: pointer;
                    }
                </style>
            </head>

            <body>
                <main>
                    <h1>MiniPedidos V2</h1>
                    <h2>Recuperar acceso administrador</h2>

                    <p>
                        Establezca una nueva contrasena
                        para la cuenta admin.
                    </p>

                    <form method="post"
                          action="/recovery/admin">

                        <input type="hidden"
                               name="%s"
                               value="%s">

                        <label for="recoveryKey">
                            Clave temporal de recuperacion
                        </label>

                        <input type="password"
                               id="recoveryKey"
                               name="recoveryKey"
                               autocomplete="off"
                               required>

                        <label for="password">
                            Nueva contrasena
                        </label>

                        <input type="password"
                               id="password"
                               name="password"
                               minlength="12"
                               autocomplete="new-password"
                               required>

                        <label for="confirmPassword">
                            Confirmar contrasena
                        </label>

                        <input type="password"
                               id="confirmPassword"
                               name="confirmPassword"
                               minlength="12"
                               autocomplete="new-password"
                               required>

                        <button type="submit">
                            Restablecer contrasena
                        </button>
                    </form>
                </main>
            </body>
            </html>
            """.formatted(
                    csrf.getParameterName(),
                    csrf.getToken()
            );

        return ResponseEntity.ok()
                .contentType(MediaType.TEXT_HTML)
                .header(HttpHeaders.CACHE_CONTROL, "no-store")
                .body(html);
    }

    // ==========================================
    // 2. ACTUALIZAR CONTRASENA EN H2
    // ==========================================

    @Transactional
    @PostMapping("/recovery/admin")
    public ResponseEntity<String> recuperar(
            @RequestParam String recoveryKey,
            @RequestParam String password,
            @RequestParam String confirmPassword) {

        if (!recuperacionHabilitada()) {
            return pagina(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "La recuperacion no esta habilitada."
            );
        }

        if (!claveValida(recoveryKey)) {
            return pagina(
                    HttpStatus.FORBIDDEN,
                    "Clave de recuperacion incorrecta."
            );
        }

        if (!usuarios.userExists("admin")) {
            return pagina(
                    HttpStatus.NOT_FOUND,
                    "La cuenta administradora no existe."
            );
        }

        if (password == null
                || password.length() < 12
                || password.getBytes(
                        StandardCharsets.UTF_8
                ).length > 72) {

            return pagina(
                    HttpStatus.BAD_REQUEST,
                    "La contrasena debe tener al menos 12 caracteres y no superar 72 bytes."
            );
        }

        if (!password.equals(confirmPassword)) {
            return pagina(
                    HttpStatus.BAD_REQUEST,
                    "Las contrasenas no coinciden."
            );
        }

        // Recuperar el usuario y conservar sus roles
        UserDetails actual =
                usuarios.loadUserByUsername("admin");

        UserDetails actualizado =
                User.withUserDetails(actual)
                    .password(encoder.encode(password))
                    .build();

        usuarios.updateUser(actualizado);

        // Volver al inicio de sesion
        return ResponseEntity
                .status(HttpStatus.SEE_OTHER)
                .header(HttpHeaders.LOCATION, "/login")
                .build();
    }

    // ==========================================
    // 3. COMPROBAR CLAVE TEMPORAL
    // ==========================================

    private boolean recuperacionHabilitada() {
        return claveRecuperacion != null
                && !claveRecuperacion.isBlank();
    }

    private boolean claveValida(String recibida) {

        if (recibida == null) {
            return false;
        }

        return MessageDigest.isEqual(
                claveRecuperacion.getBytes(
                        StandardCharsets.UTF_8
                ),
                recibida.getBytes(
                        StandardCharsets.UTF_8
                )
        );
    }

    // ==========================================
    // 4. MOSTRAR MENSAJES
    // ==========================================

    private ResponseEntity<String> pagina(
            HttpStatus estado,
            String mensaje) {

        String html = """
            <!DOCTYPE html>
            <html lang="es">
            <head>
                <meta charset="UTF-8">
                <title>MiniPedidos V2</title>
            </head>
            <body>
                <h2>MiniPedidos V2</h2>
                <p>%s</p>
                <a href="/login">Ir al inicio de sesion</a>
            </body>
            </html>
            """.formatted(mensaje);

        return ResponseEntity.status(estado)
                .contentType(MediaType.TEXT_HTML)
                .header(HttpHeaders.CACHE_CONTROL, "no-store")
                .body(html);
    }
}
