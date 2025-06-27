terraform {
  required_providers {
    clickhouse-schema = {
      source  = "erwanncloarec/clickhouse-schema"
      version = "0.1.0"
    }
  }
}

provider "clickhouse-schema" {
  host     = "localhost"
  port     = 9000
  username = "default"
  password = ""
  database = "default"
}

# Example resources (to be implemented)
# resource "clickhouse-schema_database" "example" {
#   name = "example_db"
# }

# resource "clickhouse-schema_table" "example" {
#   database = clickhouse-schema_database.example.name
#   name     = "example_table"
#   engine   = "MergeTree"
#   columns = [
#     {
#       name = "id"
#       type = "UInt64"
#     },
#     {
#       name = "timestamp"
#       type = "DateTime"
#     },
#     {
#       name = "message"
#       type = "String"
#     }
#   ]
#   order_by = ["id"]
# }