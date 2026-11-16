
package cl.minipedidos.kafka.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import org.springframework.http.HttpMethod;

import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

import static org.springframework.security.config.Customizer.withDefaults;

@Configuration
public class SecurityConfig {

    // =========================================
    // 1. CIFRADO DE CONTRASENAS
    // =========================================

    @Bean
    public PasswordEncoder passwordEncoder() {

        return new BCryptPasswordEncoder();
    }

    // =========================================
    // 2. USUARIOS Y ROLES DEL LABORATORIO
    // =========================================

    @Bean
    public UserDetailsService userDetailsService(
            PasswordEncoder encoder,

            @Value("${MINIPEDIDOS_ADMIN_PASSWORD:}")
            String claveAdmin,

            @Value("${MINIPEDIDOS_ESTUDIANTE_PASSWORD:}")
            String claveEstudiante) {

        if (claveAdmin.isBlank()
                || claveEstudiante.isBlank()) {

            throw new IllegalStateException(
                "Configure las contrasenas del laboratorio."
            );
        }

        UserDetails administrador = User.builder()
                .username("admin")
                .password(encoder.encode(claveAdmin))
                .roles("ADMIN")
                .build();

        UserDetails estudiante = User.builder()
                .username("estudiante")
                .password(encoder.encode(claveEstudiante))
                .roles("ESTUDIANTE")
                .build();

        return new InMemoryUserDetailsManager(
                administrador,
                estudiante
        );
    }

    // =========================================
    // 3. AUTORIZACION DE LAS OPERACIONES
    // =========================================

    @Bean
    public SecurityFilterChain securityFilterChain(
            HttpSecurity http) throws Exception {

        http.authorizeHttpRequests(auth -> auth

                // Consultas administrativas
                .requestMatchers(
                        HttpMethod.GET,
                        "/api/admin/kafka/**"
                )
                .hasAnyRole("ADMIN", "ESTUDIANTE")

                // Operaciones que modifican Kafka
                .requestMatchers(
                        HttpMethod.POST,
                        "/api/admin/kafka/**"
                )
                .hasRole("ADMIN")

                .requestMatchers(
                        HttpMethod.PUT,
                        "/api/admin/kafka/**"
                )
                .hasRole("ADMIN")

                .requestMatchers(
                        HttpMethod.DELETE,
                        "/api/admin/kafka/**"
                )
                .hasRole("ADMIN")

                // Acceso al dashboard
                .requestMatchers("/admin/**")
                .authenticated()

                // Denegar otros metodos administrativos
                .requestMatchers("/api/admin/kafka/**")
                .denyAll()

                // Conservar las rutas existentes
                // de MiniPedidos V2
                .anyRequest()
                .permitAll()
        );

        // Formulario de inicio de sesion
        http.formLogin(withDefaults());

        // Permitir cerrar sesion
        http.logout(withDefaults());

        // CSRF permanece habilitado
        return http.build();
    }
}
