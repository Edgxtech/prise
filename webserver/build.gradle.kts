import org.jetbrains.kotlin.gradle.tasks.KotlinJvmCompile

plugins {
    java
    kotlin("jvm") version "2.0.20"
    id("org.springframework.boot") version "3.4.3"
    id("io.spring.dependency-management") version "1.1.6"
    id("org.jetbrains.kotlin.plugin.spring") version "2.0.20"
    idea
}

version = "0.1.0"
description = "Prise Webserver"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-data-jpa:3.4.3")
    implementation("org.springframework.boot:spring-boot-starter-web:3.4.3")
    implementation("org.springframework.boot:spring-boot-starter-security:3.4.3")
    implementation("org.springframework.boot:spring-boot-starter-validation:3.4.3")
    implementation("org.springframework.boot:spring-boot-starter-cache")
    runtimeOnly("org.postgresql:postgresql")
    implementation("com.google.code.gson:gson:2.11.0")
    implementation("com.bloxbean.cardano:cardano-client-lib:0.6.3") {
        exclude(group = "javax.servlet")
    }
    implementation("io.github.microutils:kotlin-logging-jvm:3.0.5")
    implementation("org.springdoc:springdoc-openapi-starter-webmvc-ui:2.5.0")
    implementation("org.springframework.data:spring-data-redis:3.4.3")
    implementation("io.lettuce:lettuce-core:6.4.0.RELEASE")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8:2.0.0")
    implementation("org.jetbrains.kotlin:kotlin-reflect:2.0.0")
    testImplementation("org.springframework.boot:spring-boot-starter-test:3.4.3")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.2")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.2")
    testImplementation("org.junit.platform:junit-platform-commons:1.10.2")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5:2.0.0")

    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.17.0")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.17.0")
}

tasks.named<KotlinJvmCompile>("compileKotlin"){
    compilerOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        //jvmTarget = JvmTarget.JVM_17
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

// Default `test` task runs only fast, hermetic unit tests.
// Integration tests (tagged "integration") require a running webserver
// (ApiControllerIT hits http://localhost:8092) and fail with ConnectException
// when it isn't up, so they are excluded from `test` and run on-demand via
// `integrationTest`.
tasks.named<Test>("test") {
    useJUnitPlatform {
        excludeTags("integration")
    }
}

tasks.register<Test>("integrationTest") {
    description = "Runs integration tests that require a running webserver."
    group = "verification"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform {
        includeTags("integration")
    }
}

sourceSets {
    main {
        java.srcDirs("src/main/java", "src/main/kotlin")
    }
}