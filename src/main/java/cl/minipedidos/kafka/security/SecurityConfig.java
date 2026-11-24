
package cl.minipedidos.kafka.security;

// ==========================================
// MINIPEDIDOS V2 - SESION 3 - PARTE 3
// SPRING SECURITY + H2
// AUTENTICACION Y REDIRECCION AL DASHBOARD
// ACTUALIZACION SESION 6 - CIERRE DE SESION
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

                // ----------------------------------
                // A. CONFIGURACION INICIAL
                // ----------------------------------

                .requestMatchers("/setup/admin")
                .permitAll()

                // ----------------------------------
                // B. RECUPERACION DE CONTRASENA
                // ----------------------------------

                .requestMatchers("/recovery/admin")
                .permitAll()

                // ----------------------------------
                // C. CONSULTAS KAFKA
                // ----------------------------------

                .requestMatchers(
                        HttpMethod.GET,
                        "/api/admin/kafka/**"
                )
                .hasAnyRole("ADMIN", "ESTUDIANTE")

                // ----------------------------------
                // D. CREACION DE TOPICOS
                // ----------------------------------

                .requestMatchers(
                        HttpMethod.POST,
                        "/api/admin/kafka/**"
                )
                .hasRole("ADMIN")

                // ----------------------------------
                // E. MODIFICACION DE TOPICOS
                // ----------------------------------

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

                // ----------------------------------
                // F. ADMINISTRACION DE USUARIOS
                // ----------------------------------

                .requestMatchers(
                        "/api/admin/usuarios/**"
                )
                .hasRole("ADMIN")

                .requestMatchers(
                        "/admin/usuarios/**"
                )
                .hasRole("ADMIN")

                // ----------------------------------
                // G. DASHBOARD
                // ----------------------------------

                .requestMatchers("/admin/**")
                .authenticated()

                // ----------------------------------
                // H. OTRAS RUTAS ADMINISTRATIVAS
                // ----------------------------------

                .requestMatchers("/api/admin/**")
                .denyAll()

                // ----------------------------------
                // I. RUTAS EXISTENTES
                // ----------------------------------

                .anyRequest()
                .permitAll()
        );

        // ==========================================
        // 4. INICIO DE SESION
        // ==========================================

        http.formLogin(form -> form

                // Redirigir al dashboard despues
                // de iniciar sesion correctamente.
                // Se utiliza una ruta relativa
                // al servidor, no localhost.
                .defaultSuccessUrl(
                        "/admin/index.html",
                        true
                )

                .permitAll()
        );

        // ==========================================
        // 5. CIERRE DE SESION
        // ACTUALIZACION SESION 6
        // ==========================================

        http.logout(logout -> logout

                // Redirigir al formulario de login
                // despues de cerrar la sesion.
                // Se elimina ?logout para evitar
                // la respuesta HTTP 404 observada.
                .logoutSuccessUrl("/login")

                .permitAll()
        );

        // ==========================================
        // 6. PROTECCION CSRF
        // ==========================================

        // Se conserva la proteccion CSRF
        // para formularios y operaciones POST.

        return http.build();
    }
}
