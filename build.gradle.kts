import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.5.10"
    id("org.jmailen.kotlinter") version "3.4.5"
}

group = "lime"
version = "1.0"
val artifactID = "kda"

repositories {
    jcenter()
    mavenCentral()
}

dependencies {
    testImplementation("org.jetbrains.kotlin:kotlin-test:1.5.21")
    implementation("org.jetbrains.exposed:exposed-core:0.33.1")
    implementation("org.jetbrains.exposed:exposed-java-time:0.33.1")
    implementation("org.jetbrains.exposed:exposed-jdbc:0.33.1")

    testImplementation("org.postgresql", "postgresql", "42.2.16")
    implementation("org.xerial:sqlite-jdbc:3.36.0.2")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile>() {
    kotlinOptions.jvmTarget = "11"
}

kotlinter {
    indentSize = 2
    disabledRules = arrayOf("no-wildcard-imports")
}