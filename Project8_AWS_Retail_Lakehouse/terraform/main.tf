terraform {
  required_version = ">= 1.6.0"
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "4.5.0"
    }
  }
}

provider "docker" {}

variable "postgres_password" {
  type      = string
  sensitive = true
}

variable "minio_secret_key" {
  type      = string
  sensitive = true
}

resource "docker_image" "postgres" {
  name         = "postgres:16-alpine"
  keep_locally = true
}

resource "docker_image" "minio" {
  name         = "minio/minio:RELEASE.2025-04-22T22-12-26Z"
  keep_locally = true
}

resource "docker_volume" "postgres" { name = "project8-terraform-postgres" }
resource "docker_volume" "minio" { name = "project8-terraform-minio" }

resource "docker_container" "postgres" {
  name  = "project8-postgres"
  image = docker_image.postgres.image_id
  env = [
    "POSTGRES_DB=retail_lakehouse",
    "POSTGRES_USER=retail_user",
    "POSTGRES_PASSWORD=${var.postgres_password}",
  ]
  ports { internal = 5432 external = 5434 }
  volumes {
    volume_name    = docker_volume.postgres.name
    container_path = "/var/lib/postgresql/data"
  }
  restart = "unless-stopped"
}

resource "docker_container" "minio" {
  name    = "project8-minio"
  image   = docker_image.minio.image_id
  command = ["server", "/data", "--console-address", ":9001"]
  env = [
    "MINIO_ROOT_USER=lakehouse_admin",
    "MINIO_ROOT_PASSWORD=${var.minio_secret_key}",
  ]
  ports { internal = 9000 external = 9000 }
  ports { internal = 9001 external = 9001 }
  volumes {
    volume_name    = docker_volume.minio.name
    container_path = "/data"
  }
  restart = "unless-stopped"
}

output "minio_console" { value = "http://localhost:9001" }
output "postgres_port" { value = 5434 }
