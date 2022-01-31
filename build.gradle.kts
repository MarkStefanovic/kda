plugins {
    kotlin("jvm") version "1.6.10"
    id("org.jmailen.kotlinter") version "3.8.0"
    id( "org.jetbrains.kotlin.plugin.serialization") version "1.6.10"
    id("org.jetbrains.kotlinx.kover") version "0.5.0-RC2"
    id("com.github.ben-manes.versions") version "0.41.0"
}

group = "lime"
version = "1.0"
val artifactID = "kda"
val exposedVersion = "0.36.1"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.jetbrains.kotlin:kotlin-test:1.6.10")

    testImplementation("org.xerial:sqlite-jdbc:3.36.0.3")

    testImplementation("org.postgresql", "postgresql", "42.3.1")

    testImplementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.3.2")
}

tasks.test {
    useJUnitPlatform()
}

tasks.compileKotlin {
    kotlinOptions.jvmTarget = "16"
}

tasks.compileTestKotlin {
    kotlinOptions.jvmTarget = "16"
}

tasks.compileJava {
    targetCompatibility = "16"
}

tasks.compileTestJava {
    targetCompatibility = "16"
}

kotlinter {
    indentSize = 2
    disabledRules = arrayOf("no-wildcard-imports")
}
