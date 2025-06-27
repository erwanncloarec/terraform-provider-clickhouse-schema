package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// Ensure provider defined types fully satisfy framework interfaces.
var _ resource.Resource = &TableResource{}
var _ resource.ResourceWithImportState = &TableResource{}

func NewTableResource() resource.Resource {
	return &TableResource{}
}

// TableResource defines the resource implementation.
type TableResource struct {
	// client *clickhouse.Client // We'll add this later
}

// TableResourceModel describes the resource data model.
type TableResourceModel struct {
	ID       types.String   `tfsdk:"id"`
	Name     types.String   `tfsdk:"name"`
	Database types.String   `tfsdk:"database"`
	Engine   types.String   `tfsdk:"engine"`
	Columns  []ColumnModel  `tfsdk:"columns"`
	OrderBy  []types.String `tfsdk:"order_by"`
}

type ColumnModel struct {
	Name    types.String `tfsdk:"name"`
	Type    types.String `tfsdk:"type"`
	Comment types.String `tfsdk:"comment"`
}

func (r *TableResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_table"
}

func (r *TableResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		// This description is used by the documentation generator and the language server.
		MarkdownDescription: "ClickHouse table resource",

		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Table identifier",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"name": schema.StringAttribute{
				MarkdownDescription: "Table name",
				Required:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"database": schema.StringAttribute{
				MarkdownDescription: "Database name where the table will be created",
				Optional:            true,
				Computed:            true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"engine": schema.StringAttribute{
				MarkdownDescription: "Table engine (e.g., MergeTree, Log, Memory)",
				Required:            true,
			},
			"order_by": schema.ListAttribute{
				MarkdownDescription: "Columns to order by (required for MergeTree family engines)",
				Optional:            true,
				ElementType:         types.StringType,
			},
		},
		Blocks: map[string]schema.Block{
			"columns": schema.ListNestedBlock{
				MarkdownDescription: "Table columns definition",
				NestedObject: schema.NestedBlockObject{
					Attributes: map[string]schema.Attribute{
						"name": schema.StringAttribute{
							MarkdownDescription: "Column name",
							Required:            true,
						},
						"type": schema.StringAttribute{
							MarkdownDescription: "Column type (e.g., UInt64, String, DateTime)",
							Required:            true,
						},
						"comment": schema.StringAttribute{
							MarkdownDescription: "Column comment",
							Optional:            true,
						},
					},
				},
			},
		},
	}
}

func (r *TableResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	// Prevent panic if the provider has not been configured.
	if req.ProviderData == nil {
		return
	}

	// TODO: Configure ClickHouse client here
	// client, ok := req.ProviderData.(*clickhouse.Client)
	// if !ok {
	//     resp.Diagnostics.AddError(
	//         "Unexpected Resource Configure Type",
	//         fmt.Sprintf("Expected *clickhouse.Client, got: %T", req.ProviderData),
	//     )
	//     return
	// }
	// r.client = client
}

func (r *TableResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data TableResourceModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Set default database if not provided
	if data.Database.IsNull() || data.Database.IsUnknown() {
		data.Database = types.StringValue("default")
	}

	// Generate the CREATE TABLE SQL
	createSQL := r.generateCreateTableSQL(data)

	// Show the SQL that would be executed
	resp.Diagnostics.AddWarning(
		"SQL to be executed",
		fmt.Sprintf("The following SQL will be executed:\n\n%s", createSQL),
	)

	// TODO: Execute the SQL against ClickHouse
	// For now, we'll just simulate success
	fmt.Printf("Executing SQL: %s\n", createSQL)

	// Set the ID (combination of database and table name)
	data.ID = types.StringValue(fmt.Sprintf("%s.%s", data.Database.ValueString(), data.Name.ValueString()))

	// Save data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *TableResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data TableResourceModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// TODO: Query ClickHouse to check if table exists and get its schema
	// For now, we'll assume the table exists
	fmt.Printf("Would check if table %s exists\n", data.ID.ValueString())

	// Save updated data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *TableResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data TableResourceModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// TODO: Implement table updates (ALTER TABLE statements)
	// For now, we'll just log what would be updated
	fmt.Printf("Would update table %s\n", data.ID.ValueString())

	// Save updated data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *TableResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data TableResourceModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// TODO: Execute DROP TABLE statement
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s",
		data.Database.ValueString(),
		data.Name.ValueString())

	fmt.Printf("Would execute SQL: %s\n", dropSQL)
}

func (r *TableResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	// TODO: Implement import functionality
	// For now, we'll use the ID as the import identifier
	resp.Diagnostics.AddError(
		"Import Not Implemented",
		"Table import is not yet implemented",
	)
}

// generateCreateTableSQL generates the CREATE TABLE SQL statement
func (r *TableResource) generateCreateTableSQL(data TableResourceModel) string {
	sql := fmt.Sprintf("CREATE TABLE %s.%s (\n",
		data.Database.ValueString(),
		data.Name.ValueString())

	// Add columns
	for i, col := range data.Columns {
		if i > 0 {
			sql += ",\n"
		}
		sql += fmt.Sprintf("    %s %s", col.Name.ValueString(), col.Type.ValueString())

		if !col.Comment.IsNull() && !col.Comment.IsUnknown() {
			sql += fmt.Sprintf(" COMMENT '%s'", col.Comment.ValueString())
		}
	}

	sql += fmt.Sprintf("\n) ENGINE = %s", data.Engine.ValueString())

	// Add ORDER BY clause if specified (needed for MergeTree engines)
	if len(data.OrderBy) > 0 {
		sql += "\nORDER BY ("
		for i, orderCol := range data.OrderBy {
			if i > 0 {
				sql += ", "
			}
			sql += orderCol.ValueString()
		}
		sql += ")"
	}

	return sql
}
