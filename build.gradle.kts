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
    testImplementation(kotlin("test"))
    implementation("org.postgresql", "postgresql", "42.2.16")
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