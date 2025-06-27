package provider

import (
	"context"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
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

	// TODO: Create ClickHouse client configuration
	// Store client in resp.ResourceData and resp.DataSourceData for resources/data sources to use
}

func (p *clickhouseSchemaProvider) Resources(ctx context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		// TODO: Add your resources here
		// NewTableResource,
		// NewDatabaseResource,
	}
}

func (p *clickhouseSchemaProvider) DataSources(ctx context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{
		// TODO: Add your data sources here
		// NewTableDataSource,
		// NewDatabaseDataSource,
	}
}
