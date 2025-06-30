package provider

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

func New() provider.Provider {
	return &clickhouseSchemaProvider{}
}

type clickhouseSchemaProvider struct{}

type clickhouseSchemaProviderModel struct {
	Host     types.String `tfsdk:"host"`
	Port     types.Int64  `tfsdk:"port"`
	Username types.String `tfsdk:"username"`
	Password types.String `tfsdk:"password"`
	Database types.String `tfsdk:"database"`
}

func (p *clickhouseSchemaProvider) Metadata(ctx context.Context, req provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "clickhouse-schema"
}

func (p *clickhouseSchemaProvider) Schema(ctx context.Context, req provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "A Terraform provider for managing ClickHouse database schemas.",
		Attributes: map[string]schema.Attribute{
			"host": schema.StringAttribute{
				Description: "ClickHouse server host",
				Optional:    true,
			},
			"port": schema.Int64Attribute{
				Description: "ClickHouse server port",
				Optional:    true,
			},
			"username": schema.StringAttribute{
				Description: "ClickHouse username",
				Optional:    true,
			},
			"password": schema.StringAttribute{
				Description: "ClickHouse password",
				Optional:    true,
				Sensitive:   true,
			},
			"database": schema.StringAttribute{
				Description: "Default database name",
				Optional:    true,
			},
		},
	}
}

func (p *clickhouseSchemaProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var config clickhouseSchemaProviderModel
	diags := req.Config.Get(ctx, &config)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Set default values
	host := "localhost"
	if !config.Host.IsNull() && !config.Host.IsUnknown() {
		host = config.Host.ValueString()
	}

	port := int(9000)
	if !config.Port.IsNull() && !config.Port.IsUnknown() {
		port = int(config.Port.ValueInt64())
	}

	username := "default"
	if !config.Username.IsNull() && !config.Username.IsUnknown() {
		username = config.Username.ValueString()
	}

	password := ""
	if !config.Password.IsNull() && !config.Password.IsUnknown() {
		password = config.Password.ValueString()
	}

	database := "default"
	if !config.Database.IsNull() && !config.Database.IsUnknown() {
		database = config.Database.ValueString()
	}

	// Create ClickHouse connection
	conn := clickhouse.OpenDB(&clickhouse.Options{
		//Addr: []string{fmt.Sprintf("%s:%d", host, port)},
		Addr: []string{"localhost:9000"}, // Default to localhost:9000 if not specified
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})

	// Test the connection
	if err := conn.Ping(); err != nil {
		resp.Diagnostics.AddError(
			"Unable to connect to ClickHouse",
			fmt.Sprintf("Failed to connect to ClickHouse at %s:%d: %s", host, port, err.Error()),
		)
		return
	}

	tflog.Info(ctx, "Connected to ClickHouse", map[string]interface{}{
		"host":     host,
		"port":     port,
		"username": username,
		"database": database,
	})

	// Store the connection in both ResourceData and DataSourceData
	resp.ResourceData = conn
	resp.DataSourceData = conn
}

func (p *clickhouseSchemaProvider) Resources(ctx context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewTableResource,
	}
}

func (p *clickhouseSchemaProvider) DataSources(ctx context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{
		// TODO: Add your data sources here
		// NewTableDataSource,
		// NewDatabaseDataSource,
	}
}
