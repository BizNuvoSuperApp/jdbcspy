plugins {
    id("java-library")
    id("maven-publish")
}

group = "biznuvo"
version = "1.0.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.logging.log4j:log4j-api:2.25.2")

    testImplementation("javax.inject:javax.inject:1")
    testImplementation("org.testng:testng:5.14.10")
}

publishing {
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/BizNuvoSuperApp/jdbcspy")
            credentials {
                username = project.findProperty("gpr.user") as String? ?: System.getenv("GPR_USER")
                password = project.findProperty("gpr.key") as String? ?: System.getenv("GPR_KEY")
            }
        }
    }
    publications {
        register<MavenPublication>("gpr") {
            from(components["java"])
        }
    }
}
