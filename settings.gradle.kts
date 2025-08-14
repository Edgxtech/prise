rootProject.name = "prise"

include(":indexer")
include(":webserver")

project(":indexer").projectDir = file("indexer")
project(":webserver").projectDir = file("webserver")