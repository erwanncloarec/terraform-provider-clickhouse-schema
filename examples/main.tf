terraform {
  required_providers {
    clickhouse-schema = {
      source = "erwanncloarec/clickhouse-schema"
      # No version constraint when using dev_overrides
    }
  }
}

provider "clickhouse-schema" {
  host     = "localhost"
  port     = 9000
  username = "pbstck"
  password = "pbstck"
  database = "default"
}

# Example table resource
resource "clickhouse-schema_table" "example" {
  name     = "example_table"
  database = "default"
  engine   = "MergeTree"

  columns {
    name = "id"
    type = "UInt64"
    comment = "Primary key"
  }

  columns {
    name = "timestamp"
    type = "DateTime"
    comment = "Event timestamp"
  }

  columns {
    name = "message"
    type = "String"
  }

  columns {
    name = "user_id"
    type = "UInt32"
  }

  order_by = ["id"]
}