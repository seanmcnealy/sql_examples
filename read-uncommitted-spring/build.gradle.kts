plugins {
    id("java")
    `java-library`
    id("org.springframework.boot") version "3.5.5"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.mcnealysoftware"
version = "1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.mysql:mysql-connector-j:9.5.0")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.retry:spring-retry")
    testImplementation("org.springframework.boot:spring-boot-starter-test")

    testImplementation(platform("org.junit:junit-bom:5.13.4"))
    // junit-platform-launcher required https://github.com/gradle/gradle/issues/34512
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.junit.jupiter:junit-jupiter")

    testImplementation("org.testcontainers:testcontainers-mysql:2.0.2")
    testImplementation("org.testcontainers:junit-jupiter")

    // flyway-core required https://github.com/flyway/flyway/issues/4145
    testImplementation("org.flywaydb:flyway-core:11.13.1")
    implementation("org.flywaydb:flyway-mysql:11.18.0")

    testImplementation("org.springframework.boot:spring-boot-testcontainers")
}

tasks.test {
    useJUnitPlatform()
}