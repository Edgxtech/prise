package tech.edgx.prise.webserver.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.servlet.config.annotation.EnableWebMvc
import springfox.documentation.builders.ApiInfoBuilder
import springfox.documentation.builders.PathSelectors
import springfox.documentation.service.ApiInfo
import springfox.documentation.spi.DocumentationType
import springfox.documentation.spring.web.plugins.Docket
import springfox.documentation.swagger2.annotations.EnableSwagger2

@EnableSwagger2
@EnableWebMvc
@Configuration
class SwaggerConfig {
    private fun apiPublicInfo(): ApiInfo? {
        return ApiInfoBuilder()
            .title("Prise API")
            .description("Cardano Price API")
            .version("0.1")
            .contact(springfox.documentation.service.Contact("edgx.tech", "https://edgx.tech", null))
            .build()
    }

    @Bean
    fun publicApi(): Docket? {
        return Docket(DocumentationType.OAS_30)
            .apiInfo(apiPublicInfo())
            .groupName("1. public")
            .select()
            .paths(PathSelectors.any())
            .build()
    }
}