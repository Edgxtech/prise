package tech.edgx.prise.webserver.config

import io.swagger.v3.oas.models.OpenAPI
import io.swagger.v3.oas.models.info.Contact
import io.swagger.v3.oas.models.info.Info
import org.springdoc.core.models.GroupedOpenApi
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class SwaggerConfig {

    @Bean
    fun publicApi(): GroupedOpenApi {
        return GroupedOpenApi.builder()
            .group("1. public")
            .pathsToMatch("/**")
            .build()
    }

    @Bean
    fun customOpenAPI(): OpenAPI {
        return OpenAPI()
            .info(
                Info()
                    .title("Prise API")
                    .description("Cardano Price API")
                    .version("0.1")
                    .contact(
                        Contact()
                            .name("edgx.tech")
                            .url("https://edgx.tech")
                            .email(null)
                    )
            )
    }
}