package tech.edgx.prise.webserver.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.security.config.Customizer
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity
import org.springframework.security.web.SecurityFilterChain
import org.springframework.web.cors.CorsConfiguration
import org.springframework.web.cors.CorsConfigurationSource
import org.springframework.web.cors.UrlBasedCorsConfigurationSource

@Configuration
@EnableWebSecurity
class SecurityConfig {

    @Value("\${cors.app.address}")
    lateinit var corsAppAddress: String

    @Bean
    fun corsConfigurationSource(): CorsConfigurationSource? {
        println("Using app address: $corsAppAddress")
        val configuration = CorsConfiguration()
        configuration.allowedOrigins = listOf(corsAppAddress)
        configuration.allowedMethods = listOf("GET", "HEAD", "OPTIONS")
        configuration.allowedHeaders = listOf("Origin", "X-Requested-With", "Content-Type", "Accept")
        val source = UrlBasedCorsConfigurationSource()
        source.registerCorsConfiguration("/**", configuration)
        return source
    }

    @Bean
    fun filterChain(httpSecurity: HttpSecurity): SecurityFilterChain {
        httpSecurity
            .cors(Customizer.withDefaults())
            .csrf { it.disable() }
            .authorizeHttpRequests { auth ->
                auth
                    .requestMatchers("/**").permitAll()
                    .requestMatchers("/").permitAll()
            }
        return httpSecurity.build()
    }
}