import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    java
    kotlin("jvm") version "1.9.23"
    `maven-publish`
}

group = "tech.edgx.prise"
version = "0.0.1"
description = "Prise Indexer"
java.sourceCompatibility = JavaVersion.VERSION_11
java.targetCompatibility = JavaVersion.VERSION_11

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.org.jetbrains.kotlin.kotlin.stdlib.jdk8)
    implementation(libs.org.jetbrains.kotlinx.kotlinx.coroutines.core)
    implementation(libs.org.ktorm.ktorm.core)
    implementation(libs.org.ktorm.ktorm.support.mysql)
    implementation(libs.mysql.mysql.connector.java)
    implementation(libs.org.postgresql.postgresql)
    implementation(libs.com.zaxxer.hikaricp)
    implementation(libs.org.slf4j.slf4j.api)
    implementation(libs.ch.qos.logback.logback.classic)
    implementation(libs.ch.qos.logback.logback.core)
    implementation(libs.com.google.code.gson.gson)
    implementation(libs.javax.xml.bind.jaxb.api)
    implementation(libs.com.bloxbean.cardano.cardano.client.lib)
    implementation(libs.io.micrometer.micrometer.registry.prometheus)
    implementation(libs.org.apache.commons.commons.math)
    implementation(libs.com.google.guava.guava)
    implementation(libs.com.bloxbean.cardano.yaci)
    implementation(libs.io.insert.koin.koin.core)
    implementation(libs.io.insert.koin.koin.core.jvm)
    implementation(libs.org.quartz.scheduler.quartz)
    implementation(libs.org.codehaus.janino.janino)
    testImplementation(libs.org.junit.jupiter.junit.jupiter)
    testImplementation(libs.io.mockk.mockk)
    testImplementation(libs.io.insert.koin.koin.test.junit5)
    testImplementation(libs.it.skrape.skrapeit)
}

val compileKotlin: KotlinCompile by tasks
compileKotlin.kotlinOptions {
    jvmTarget = "11"
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
    manifest.attributes["Main-Class"] = "tech.edgx.prise.indexer.PriseRunner" //"tech.edgx.prise.indexer.PriseIndexerKt"
    // Needed otherwise complains no main class
    exclude("META-INF/*.RSA", "META-INF/*.SF", "META-INF/*.DSA")
    val dependencies = configurations
        .runtimeClasspath
        .get()
        .map(::zipTree) // OR .map { zipTree(it) }
    from(dependencies)
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

publishing {
    publications.create<MavenPublication>("maven") {
        from(components["java"])
    }
}

tasks.withType<JavaCompile>() {
    options.encoding = "UTF-8"
}

tasks.withType<Javadoc>() {
    options.encoding = "UTF-8"
}