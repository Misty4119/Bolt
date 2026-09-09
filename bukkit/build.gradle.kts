plugins {
    id("com.modrinth.minotaur") version "2.+"
}

repositories {
    mavenCentral()
    maven("https://maven.canvasmc.io/releases")
    maven("https://repo.papermc.io/repository/maven-public/")
    maven("https://repo.codemc.io/repository/maven-public/")
    maven("https://s01.oss.sonatype.org/content/repositories/snapshots/")
}

dependencies {
    compileOnly(group = "io.canvasmc.canvas", name = "canvas-api", version = "26.2.build.923-stable")
    implementation("com.zaxxer:HikariCP:${project.property("hikariVersion")}")
    implementation("org.postgresql:postgresql:${project.property("postgresqlDriverVersion")}")
    implementation("com.mysql:mysql-connector-j:${project.property("mysqlDriverVersion")}")
    implementation("org.xerial:sqlite-jdbc:${project.property("sqliteDriverVersion")}")
    implementation("io.lettuce:lettuce-core:${project.property("lettuceVersion")}")
    implementation(group = "net.kyori", name = "event-api", version = "3.0.0") {
        exclude(module = "guava")
        exclude(module = "checker-qual")
    }
    implementation(group = "org.bstats", name = "bstats-bukkit", version = "3.0.2")
    implementation(group = "org.popcraft", name = "chunky-nbt", version = "1.3.127")
    api(project(":bolt-common"))
    implementation(project(":bolt-folia"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
}

tasks {
    test {
        useJUnitPlatform()
    }
}

tasks {
    processResources {
        inputs.property("version", project.version)
        val name = project.property("artifactName")
        val version = project.version
        val group = project.group
        val description = project.property("description")
        filesMatching("plugin.yml") {
            expand(
                "name" to name,
                "version" to version,
                "group" to group,
                "description" to description,
            )
        }
    }
    shadowJar {
        relocate("net.kyori.event", "${project.group}.${rootProject.name}.lib.net.kyori.event")
        relocate("org.bstats", "${project.group}.${rootProject.name}.lib.org.bstats")
        relocate("org.popcraft.chunky.nbt", "${project.group}.${rootProject.name}.lib.org.popcraft.chunky.nbt")
        relocate("com.zaxxer.hikari", "${project.group}.${rootProject.name}.lib.com.zaxxer.hikari")
        relocate("io.lettuce", "${project.group}.${rootProject.name}.lib.io.lettuce")
        relocate("io.netty", "${project.group}.${rootProject.name}.lib.io.netty")
        manifest {
            attributes("paperweight-mappings-namespace" to "mojang")
        }
    }
}

modrinth {
    token.set(System.getenv("MODRINTH_TOKEN"))
    projectId.set("bolt")
    versionName.set("${project.property("artifactName")} ${project.version}")
    versionNumber.set("${project.version}")
    versionType.set("release")
    uploadFile.set(tasks.shadowJar)
    gameVersions.addAll(
        "1.18.2",
        "1.19",
        "1.19.1",
        "1.19.2",
        "1.19.3",
        "1.19.4",
        "1.20",
        "1.20.1",
        "1.20.2",
        "1.20.3",
        "1.20.4",
        "1.20.5",
        "1.20.6",
        "1.21",
        "1.21.1",
        "1.21.2",
        "1.21.3",
        "1.21.4"
    )
    loaders.addAll("bukkit", "spigot", "paper", "folia")
}
