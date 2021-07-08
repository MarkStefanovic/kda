import org.jetbrains.compose.compose
import org.jetbrains.compose.desktop.application.dsl.TargetFormat
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.5.10"
    id("org.jetbrains.compose") version "0.4.0"
    jacoco
}

group = "lime"
version = "1.0"

repositories {
    jcenter()
    mavenCentral()
    maven { url = uri("https://maven.pkg.jetbrains.space/public/p/compose/dev") }
}

dependencies {
    testImplementation(kotlin("test"))
    implementation(compose.desktop.currentOs)

    testImplementation("com.h2database", "h2", "1.4.200")
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

compose.desktop {
    application {
        mainClass = "MainKt"
        nativeDistributions {
            targetFormats(TargetFormat.Dmg, TargetFormat.Msi, TargetFormat.Deb)
            packageName = "kda"
            packageVersion = "1.0.0"
        }
    }
}
