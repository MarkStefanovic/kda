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

    testImplementation("org.xerial:sqlite-jdbc:3.36.0.2")

    testImplementation("org.postgresql", "postgresql", "42.2.16")
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