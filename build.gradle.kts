import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.5.10"
    jacoco
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

    finalizedBy(tasks.jacocoTestReport) // jacoco test coverage report is always generated after tests run
}

tasks.jacocoTestReport {
    dependsOn(tasks.test) // tests are required to run before generating the report
}

tasks.withType<KotlinCompile>() {
    kotlinOptions.jvmTarget = "11"
}

jacoco {
    toolVersion = "0.8.7"
}

