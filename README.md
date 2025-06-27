# Terraform Plugin for ClickHouse Schema Management

### Development Overrides Setup
Create a .terraformrc file in your home directory for local development:

```hcl
provider_installation {
  dev_overrides {
    "erwanncloarec/clickhouse-schema" = "/path/to/your/terraform-provider-clickhouse-schema"
  }

  # For all other providers, install them directly from their origin provider
  # registries as normal.
  direct {
    exclude = ["erwanncloarec/clickhouse-schema"]
  }
}
```