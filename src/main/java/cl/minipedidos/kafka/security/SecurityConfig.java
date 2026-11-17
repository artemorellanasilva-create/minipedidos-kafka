
package cl.minipedidos.kafka.security;

// ==========================================
// MINIPEDIDOS V2 - SESION 3 - PARTE 3
// SEGURIDAD CON SPRING SECURITY Y H2
// ==========================================

import javax.sql.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;

import org.springframework.security.config.annotation.web.builders.HttpSecurity;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

import org.springframework.security.provisioning.JdbcUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

import static org.springframework.security.config.Customizer.withDefaults;

@Configuration
public class SecurityConfig {

    // ==========================================
    // 1. CIFRADO DE CONTRASENAS
    // ==========================================

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    // ==========================================
    // 2. USUARIOS PERSISTENTES EN H2
    // ==========================================

    @Bean
    public JdbcUserDetailsManager userDetailsService(
            DataSource dataSource) {

        JdbcUserDetailsManager usuarios =
                new JdbcUserDetailsManager(dataSource);

        usuarios.setUsersByUsernameQuery(
                "SELECT username, password, enabled " +
                "FROM users WHERE username = ?"
        );

        usuarios.setAuthoritiesByUsernameQuery(
                "SELECT username, authority " +
                "FROM authorities WHERE username = ?"
        );

        return usuarios;
    }

    // ==========================================
    // 3. SEGURIDAD Y AUTORIZACION
    // ==========================================

    @Bean
    public SecurityFilterChain securityFilterChain(
            HttpSecurity http) throws Exception {

        http.authorizeHttpRequests(auth -> auth

                // Configuracion inicial del administrador.
                // El controlador verifica la clave de
                // activacion y si ya existe la cuenta.
                .requestMatchers("/setup/admin")
                .permitAll()

                // Consultas de topicos
                .requestMatchers(
                        HttpMethod.GET,
                        "/api/admin/kafka/**"
                )
                .hasAnyRole("ADMIN", "ESTUDIANTE")

                // Creacion de topicos
                .requestMatchers(
                        HttpMethod.POST,
                        "/api/admin/kafka/**"
                )
                .hasRole("ADMIN")

                // Modificacion de topicos
                .requestMatchers(
                        HttpMethod.PUT,
                        "/api/admin/kafka/**"
                )
                .hasRole("ADMIN")

                .requestMatchers(
                        HttpMethod.PATCH,
                        "/api/admin/kafka/**"
                )
                .hasRole("ADMIN")

                .requestMatchers(
                        HttpMethod.DELETE,
                        "/api/admin/kafka/**"
                )
                .hasRole("ADMIN")

                // Gestion de usuarios
                .requestMatchers(
                        "/api/admin/usuarios/**"
                )
                .hasRole("ADMIN")

                .requestMatchers(
                        "/admin/usuarios/**"
                )
                .hasRole("ADMIN")

                // Dashboard
                .requestMatchers("/admin/**")
                .authenticated()

                // Otras rutas administrativas
                .requestMatchers("/api/admin/**")
                .denyAll()

                // Rutas existentes de MiniPedidos
                .anyRequest()
                .permitAll()
        );

        // Inicio de sesion
        http.formLogin(withDefaults());

        // Cierre de sesion
        http.logout(withDefaults());

        // Mantener proteccion CSRF activada
        return http.build();
    }
}
