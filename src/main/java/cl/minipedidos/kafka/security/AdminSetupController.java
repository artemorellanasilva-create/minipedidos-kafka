
package cl.minipedidos.kafka.security;

// ==========================================
// MINIPEDIDOS V2 - SESION 3 - PARTE 3
// CONFIGURACION INICIAL DEL ADMINISTRADOR
// ==========================================

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Value;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import org.springframework.security.core.userdetails.User;
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
public class AdminSetupController {

    private final JdbcUserDetailsManager usuarios;
    private final PasswordEncoder encoder;
    private final String claveActivacion;

    // ==========================================
    // 1. CONSTRUCTOR
    // ==========================================

    public AdminSetupController(
            JdbcUserDetailsManager usuarios,
            PasswordEncoder encoder,
            @Value("${MINIPEDIDOS_SETUP_KEY:}")
            String claveActivacion) {

        this.usuarios = usuarios;
        this.encoder = encoder;
        this.claveActivacion = claveActivacion;
    }

    // ==========================================
    // 2. FORMULARIO WEB DE CONFIGURACION
    // ==========================================

    @GetMapping("/setup/admin")
    public ResponseEntity<String> formulario(
            HttpServletRequest request) {

        if (usuarios.userExists("admin")) {
            return pagina(
                    HttpStatus.GONE,
                    "La cuenta administradora ya existe."
            );
        }

        if (claveActivacion == null
                || claveActivacion.isBlank()) {

            return pagina(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "La configuracion inicial no esta habilitada."
            );
        }

        CsrfToken csrf = (CsrfToken)
                request.getAttribute(
                        CsrfToken.class.getName()
                );

        if (csrf == null) {
            return pagina(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    "No fue posible preparar el formulario seguro."
            );
        }

        String html = """
            <!DOCTYPE html>
            <html lang="es">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport"
                      content="width=device-width, initial-scale=1">

                <title>MiniPedidos V2 | Configuracion inicial</title>

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

                    h1 {
                        color: #183b63;
                        font-size: 26px;
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
                        margin-top: 25px;
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

                    <h2>Crear administrador inicial</h2>

                    <p>
                        Este formulario permite configurar
                        la primera cuenta administradora.
                    </p>

                    <form method="post" action="/setup/admin">

                        <input type="hidden"
                               name="%s"
                               value="%s">

                        <label for="activationKey">
                            Clave de activacion inicial
                        </label>

                        <input type="password"
                               id="activationKey"
                               name="activationKey"
                               required
                               autocomplete="off">

                        <label for="username">
                            Usuario administrador
                        </label>

                        <input type="text"
                               id="username"
                               value="admin"
                               disabled>

                        <label for="password">
                            Nueva contrasena
                        </label>

                        <input type="password"
                               id="password"
                               name="password"
                               minlength="12"
                               required
                               autocomplete="new-password">

                        <label for="confirmPassword">
                            Confirmar contrasena
                        </label>

                        <input type="password"
                               id="confirmPassword"
                               name="confirmPassword"
                               minlength="12"
                               required
                               autocomplete="new-password">

                        <button type="submit">
                            Crear administrador
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
                .header(
                        HttpHeaders.CACHE_CONTROL,
                        "no-store"
                )
                .body(html);
    }

    // ==========================================
    // 3. REGISTRAR EL ADMINISTRADOR
    // ==========================================

    @Transactional
    @PostMapping("/setup/admin")
    public synchronized ResponseEntity<String> crearAdmin(
            @RequestParam String activationKey,
            @RequestParam String password,
            @RequestParam String confirmPassword) {

        // Impedir nuevas configuraciones
        if (usuarios.userExists("admin")) {
            return pagina(
                    HttpStatus.GONE,
                    "El administrador ya fue configurado."
            );
        }

        // Configuracion cerrada si no existe clave
        if (claveActivacion == null
                || claveActivacion.isBlank()) {

            return pagina(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "La configuracion inicial no esta habilitada."
            );
        }

        // Comprobar la clave de activacion
        if (!claveValida(activationKey)) {
            return pagina(
                    HttpStatus.FORBIDDEN,
                    "Clave de activacion incorrecta."
            );
        }

        // Validar la contrasena
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

        // Crear cuenta con contraseña BCrypt
        usuarios.createUser(
                User.builder()
                    .username("admin")
                    .password(encoder.encode(password))
                    .roles("ADMIN")
                    .build()
        );

        // Redirigir al inicio de sesion
        return ResponseEntity
                .status(HttpStatus.SEE_OTHER)
                .header(HttpHeaders.LOCATION, "/login")
                .build();
    }

    // ==========================================
    // 4. VALIDAR CLAVE DE ACTIVACION
    // ==========================================

    private boolean claveValida(String recibida) {

        if (recibida == null) {
            return false;
        }

        return MessageDigest.isEqual(
                claveActivacion.getBytes(
                        StandardCharsets.UTF_8
                ),
                recibida.getBytes(
                        StandardCharsets.UTF_8
                )
        );
    }

    // ==========================================
    // 5. RESPUESTAS HTML
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
                .header(
                        HttpHeaders.CACHE_CONTROL,
                        "no-store"
                )
                .body(html);
    }
}
