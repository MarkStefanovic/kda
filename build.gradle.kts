import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    kotlin("jvm") version "1.5.10"
    jacoco
//    `maven-publish`
//    id("com.jfrog.bintray") version "1.8.4"
    id("com.github.johnrengelman.shadow") version "5.2.0"
    id("org.jmailen.kotlinter") version "3.4.5"
}

group = "lime"
version = "1.0"

repositories {
    jcenter()
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
//    testImplementation("com.h2database", "h2", "1.4.200")
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

val artifactID = "kda"

val shadowJar: ShadowJar by tasks

kotlinter {
    indentSize = 2
}

tasks.check {
    dependsOn("installKotlinterPrePushHook")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}
