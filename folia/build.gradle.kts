repositories {
    maven("https://maven.canvasmc.io/releases")
    maven("https://repo.papermc.io/repository/maven-public/")
}

dependencies {
    compileOnly(group = "io.canvasmc.canvas", name = "canvas-api", version = "26.2.build.941-stable")
}
