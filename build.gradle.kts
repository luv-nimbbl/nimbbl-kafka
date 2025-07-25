plugins {
    kotlin("jvm") version "2.1.10"
    kotlin("plugin.serialization") version "1.9.22"
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

group = "com.nimbbl"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven {
        url = uri("https://packages.confluent.io/maven")
    }
}

dependencies {
    testImplementation(kotlin("test"))
    implementation("org.apache.kafka:kafka-clients:3.9.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
    api("io.confluent:kafka-avro-serializer:7.9.0")
    api("org.apache.avro:avro:1.11.4")
    implementation("ch.qos.logback:logback-classic:1.5.18")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(17)
}

avro {
    setCreateSetters(true)
}