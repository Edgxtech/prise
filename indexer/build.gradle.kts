import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.util.Properties

buildscript {
    repositories {
        mavenCentral()
    }
    dependencies {
        // Needed for Flyway migrations to handle jdbc:postgresql
        classpath("org.flywaydb:flyway-core:11.11.0")
        classpath("org.flywaydb:flyway-database-postgresql:10.22.0")
    }
}

plugins {
    application
    java
    kotlin("jvm") version "1.9.23"
    `maven-publish`
    id("org.flywaydb.flyway") version "11.11.0"
}

tasks.named("distZip") { enabled = false }
tasks.named("distTar") { enabled = false }

version = "0.1.0"
description = "Prise Indexer"
java.sourceCompatibility = JavaVersion.VERSION_11
java.targetCompatibility = JavaVersion.VERSION_11

repositories {
    mavenCentral()
}

application {
    mainClass.set("tech.edgx.prise.indexer.PriseRunner")
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8:1.9.21")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
    implementation("org.ktorm:ktorm-core:3.6.0")
    implementation("org.postgresql:postgresql:42.7.4")
    implementation("com.zaxxer:HikariCP:4.0.3")
    implementation("org.slf4j:slf4j-api:2.0.13")
    implementation("ch.qos.logback:logback-classic:1.5.7")
    implementation("ch.qos.logback:logback-core:1.5.7")
    implementation("com.google.code.gson:gson:2.8.9")
    implementation("javax.xml.bind:jaxb-api:2.1")
    implementation("com.bloxbean.cardano:cardano-client-lib:0.5.1")
    implementation("io.micrometer:micrometer-registry-prometheus:1.12.3")
    implementation("org.apache.commons:commons-math:2.2")
    implementation("com.google.guava:guava:33.3.1-jre")
    implementation("com.bloxbean.cardano:yaci:0.3.4.1")
    implementation("io.insert-koin:koin-core:3.5.3")
    implementation("io.insert-koin:koin-core-jvm:3.5.3")
    implementation("org.quartz-scheduler:quartz:2.3.2")
    implementation("org.codehaus.janino:janino:3.1.8")
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.1")
    testImplementation("io.mockk:mockk:1.9.3")
    testImplementation("io.insert-koin:koin-test-junit5:3.5.3")
    testImplementation("it.skrape:skrapeit:1.2.2")
    implementation("com.github.ben-manes.caffeine:caffeine:3.2.0")
    implementation("io.lettuce:lettuce-core:6.3.2.RELEASE")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.15.2")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.2")
    implementation("com.cronutils:cron-utils:9.2.1")
}
//dependencies {
//    implementation(libs.org.jetbrains.kotlin.kotlin.stdlib.jdk8)
//    implementation(libs.org.jetbrains.kotlinx.kotlinx.coroutines.core)
//    implementation(libs.org.ktorm.ktorm.core)
//    implementation("org.postgresql:postgresql:42.7.4")
//    implementation(libs.com.zaxxer.hikaricp)
//    implementation(libs.org.slf4j.slf4j.api)
//    implementation("ch.qos.logback:logback-classic:1.5.7")
//    implementation("ch.qos.logback:logback-core:1.5.7")
//    implementation(libs.com.google.code.gson.gson)
//    implementation(libs.javax.xml.bind.jaxb.api)
//    implementation(libs.com.bloxbean.cardano.cardano.client.lib)
//    implementation(libs.io.micrometer.micrometer.registry.prometheus)
//    implementation(libs.org.apache.commons.commons.math)
//    implementation(libs.com.google.guava.guava)
//    implementation(libs.com.bloxbean.cardano.yaci)
//    implementation(libs.io.insert.koin.koin.core)
//    implementation(libs.io.insert.koin.koin.core.jvm)
//    implementation(libs.org.quartz.scheduler.quartz)
//    implementation(libs.org.codehaus.janino.janino)
//    testImplementation(libs.org.junit.jupiter.junit.jupiter)
//    testImplementation(libs.io.mockk.mockk)
//    testImplementation(libs.io.insert.koin.koin.test.junit5)
//    testImplementation(libs.it.skrape.skrapeit)
//    implementation("com.github.ben-manes.caffeine:caffeine:3.2.0")
//    implementation("io.lettuce:lettuce-core:6.3.2.RELEASE")
//    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.15.2")
//    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.2")
//    implementation("com.cronutils:cron-utils:9.2.1")
//}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        jvmTarget = "11"
    }
    incremental = true
}

val compileTestKotlin: KotlinCompile by tasks
compileTestKotlin.kotlinOptions {
    jvmTarget = "11"
}

sourceSets.main {
    java.srcDirs("src/main/java", "src/main/kotlin")
}

tasks.withType<Test> {
    useJUnitPlatform()
}

tasks.jar {
    manifest.attributes["Main-Class"] = "tech.edgx.prise.indexer.PriseRunner"
    // Needed otherwise complains no main class
    exclude("META-INF/*.RSA", "META-INF/*.SF", "META-INF/*.DSA")
    val dependencies = configurations
        .runtimeClasspath
        .get()
        .map(::zipTree)
    from(dependencies)
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
        }
    }
}

// Load properties from prise.example.properties
val props = Properties().apply {
    file("prise.properties").inputStream().use { load(it) }
}

flyway {
    url = props.getProperty("app.datasource.url") ?: "jdbc:postgresql://localhost:5432/prise"
    user = props.getProperty("app.datasource.username") ?: "your_username"
    password = props.getProperty("app.datasource.password") ?: "your_password"
    baselineOnMigrate = true
    schemas = arrayOf("public")
}

tasks.withType<JavaCompile>() {
    options.encoding = "UTF-8"
}

tasks.withType<Javadoc>() {
    options.encoding = "UTF-8"
}

tasks.register("debugClasspath") {
    doLast {
        val resources = sourceSets.main.get().resources
        val migrationFiles = resources.matching { include("db/migration/**") }
        println("Migration files in classpath:")
        migrationFiles.forEach { println(it.path) }
    }
}