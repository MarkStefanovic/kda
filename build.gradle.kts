import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.5.31"
    id("org.jmailen.kotlinter") version "3.4.5"
}

group = "lime"
version = "1.0"
val artifactID = "kda"
val exposedVersion = "0.36.1"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("org.jetbrains.kotlin:kotlin-test:1.5.21")
    implementation("org.jetbrains.exposed:exposed-core:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-java-time:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-jdbc:$exposedVersion")

    testImplementation("org.postgresql", "postgresql", "42.2.16")
    implementation("org.xerial:sqlite-jdbc:3.36.0.2")

    implementation("com.zaxxer:HikariCP:5.0.0")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile>() {
    kotlinOptions.jvmTarget = "16"
}

kotlinter {
    indentSize = 2
    disabledRules = arrayOf("no-wildcard-imports")
}