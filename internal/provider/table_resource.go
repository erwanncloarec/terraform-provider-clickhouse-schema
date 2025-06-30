package provider

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

// Ensure provider defined types fully satisfy framework interfaces.
var _ resource.Resource = &TableResource{}
var _ resource.ResourceWithImportState = &TableResource{}

func NewTableResource() resource.Resource {
	return &TableResource{}
}

// TableResource defines the resource implementation.
type TableResource struct {
	client *sql.DB
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

	client, ok := req.ProviderData.(*sql.DB)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			fmt.Sprintf("Expected *sql.DB, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)
		return
	}

	r.client = client
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

	tflog.Info(ctx, "Creating ClickHouse table", map[string]interface{}{
		"sql": createSQL,
	})

	// Execute the SQL against ClickHouse
	_, err := r.client.ExecContext(ctx, createSQL)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error creating table",
			fmt.Sprintf("Could not create table %s.%s: %s",
				data.Database.ValueString(),
				data.Name.ValueString(),
				err.Error()),
		)
		return
	}

	// Set the ID (combination of database and table name)
	data.ID = types.StringValue(fmt.Sprintf("%s.%s", data.Database.ValueString(), data.Name.ValueString()))

	tflog.Info(ctx, "Successfully created ClickHouse table", map[string]interface{}{
		"id": data.ID.ValueString(),
	})

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

	// Check if table exists
	parts := strings.Split(data.ID.ValueString(), ".")
	if len(parts) != 2 {
		resp.Diagnostics.AddError(
			"Invalid table ID",
			fmt.Sprintf("Expected format 'database.table', got: %s", data.ID.ValueString()),
		)
		return
	}

	database := parts[0]
	tableName := parts[1]

	// Query to check if table exists and get engine
	tableQuery := `
        SELECT engine
        FROM system.tables 
        WHERE database = ? AND name = ?
    `

	var actualEngine string
	err := r.client.QueryRowContext(ctx, tableQuery, database, tableName).Scan(&actualEngine)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Table doesn't exist, remove from state
			tflog.Info(ctx, "Table no longer exists, removing from state", map[string]interface{}{
				"id": data.ID.ValueString(),
			})
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError(
			"Error checking table existence",
			fmt.Sprintf("Could not check if table %s exists: %s", data.ID.ValueString(), err.Error()),
		)
		return
	}

	// Validate engine matches
	if actualEngine != data.Engine.ValueString() {
		resp.Diagnostics.AddError(
			"Table engine mismatch",
			fmt.Sprintf("Expected engine '%s', but table has engine '%s'",
				data.Engine.ValueString(), actualEngine),
		)
		return
	}

	// Get actual column schema
	actualColumns, err := r.getTableColumns(ctx, database, tableName)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error reading table schema",
			fmt.Sprintf("Could not read schema for table %s: %s", data.ID.ValueString(), err.Error()),
		)
		return
	}

	// Validate columns match expected schema
	if err := r.validateColumns(data.Columns, actualColumns); err != nil {
		resp.Diagnostics.AddError(
			"Table schema mismatch",
			fmt.Sprintf("Table schema does not match configuration: %s", err.Error()),
		)
		return
	}

	// Get actual ORDER BY clause if it's a MergeTree family engine
	if r.isMergeTreeFamily(actualEngine) {
		actualOrderBy, err := r.getTableOrderBy(ctx, database, tableName)
		if err != nil {
			resp.Diagnostics.AddError(
				"Error reading table ORDER BY",
				fmt.Sprintf("Could not read ORDER BY for table %s: %s", data.ID.ValueString(), err.Error()),
			)
			return
		}

		// Validate ORDER BY matches
		if err := r.validateOrderBy(data.OrderBy, actualOrderBy); err != nil {
			resp.Diagnostics.AddError(
				"Table ORDER BY mismatch",
				fmt.Sprintf("Table ORDER BY does not match configuration: %s", err.Error()),
			)
			return
		}
	}

	tflog.Info(ctx, "Table schema validation successful", map[string]interface{}{
		"id":     data.ID.ValueString(),
		"engine": actualEngine,
	})

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

	// Not implemented yet
	resp.Diagnostics.AddError("Update is not implemented", "Update is not implemented")
}

func (r *TableResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data TableResourceModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Execute DROP TABLE statement
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s",
		data.Database.ValueString(),
		data.Name.ValueString())

	tflog.Info(ctx, "Dropping ClickHouse table", map[string]interface{}{
		"sql": dropSQL,
	})

	_, err := r.client.ExecContext(ctx, dropSQL)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error dropping table",
			fmt.Sprintf("Could not drop table %s: %s", data.ID.ValueString(), err.Error()),
		)
		return
	}

	tflog.Info(ctx, "Successfully dropped ClickHouse table", map[string]interface{}{
		"id": data.ID.ValueString(),
	})
}

func (r *TableResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	// Expected format: database.table
	parts := strings.Split(req.ID, ".")
	if len(parts) != 2 {
		resp.Diagnostics.AddError(
			"Invalid import identifier",
			fmt.Sprintf("Expected format 'database.table', got: %s", req.ID),
		)
		return
	}

	database := parts[0]
	tableName := parts[1]

	// Validate that both database and table name are not empty
	if database == "" || tableName == "" {
		resp.Diagnostics.AddError(
			"Invalid import identifier",
			"Database and table names cannot be empty",
		)
		return
	}

	tflog.Info(ctx, "Importing ClickHouse table", map[string]interface{}{
		"database": database,
		"table":    tableName,
		"id":       req.ID,
	})

	// Check if table exists and get its properties
	tableQuery := `
        SELECT engine
        FROM system.tables 
        WHERE database = ? AND name = ?
    `

	var engine string
	err := r.client.QueryRowContext(ctx, tableQuery, database, tableName).Scan(&engine)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			resp.Diagnostics.AddError(
				"Table not found",
				fmt.Sprintf("Table %s.%s does not exist in ClickHouse", database, tableName),
			)
			return
		}
		resp.Diagnostics.AddError(
			"Error checking table existence",
			fmt.Sprintf("Could not check if table %s.%s exists: %s", database, tableName, err.Error()),
		)
		return
	}

	// Get table columns
	columns, err := r.getTableColumns(ctx, database, tableName)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error reading table schema",
			fmt.Sprintf("Could not read schema for table %s.%s: %s", database, tableName, err.Error()),
		)
		return
	}

	// Convert columns map to slice for the model
	var columnModels []ColumnModel
	for _, col := range columns {
		columnModel := ColumnModel{
			Name: types.StringValue(col.Name),
			Type: types.StringValue(col.Type),
		}

		if col.Comment != "" {
			columnModel.Comment = types.StringValue(col.Comment)
		} else {
			columnModel.Comment = types.StringNull()
		}

		columnModels = append(columnModels, columnModel)
	}

	// Get ORDER BY clause if it's a MergeTree family engine
	var orderBy []types.String
	if r.isMergeTreeFamily(engine) {
		orderByColumns, err := r.getTableOrderBy(ctx, database, tableName)
		if err != nil {
			resp.Diagnostics.AddError(
				"Error reading table ORDER BY",
				fmt.Sprintf("Could not read ORDER BY for table %s.%s: %s", database, tableName, err.Error()),
			)
			return
		}

		// Convert to types.String slice
		for _, col := range orderByColumns {
			orderBy = append(orderBy, types.StringValue(col))
		}
	}

	// Create the resource model with imported data
	data := TableResourceModel{
		ID:       types.StringValue(req.ID),
		Name:     types.StringValue(tableName),
		Database: types.StringValue(database),
		Engine:   types.StringValue(engine),
		Columns:  columnModels,
		OrderBy:  orderBy,
	}

	tflog.Info(ctx, "Successfully imported ClickHouse table", map[string]interface{}{
		"id":      data.ID.ValueString(),
		"engine":  engine,
		"columns": len(columnModels),
	})

	// Set the imported state
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
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

// getTableColumns retrieves the actual column schema from ClickHouse
func (r *TableResource) getTableColumns(ctx context.Context, database, tableName string) (map[string]ColumnInfo, error) {
	query := `
        SELECT name, type, comment
        FROM system.columns
        WHERE database = ? AND table = ?
        ORDER BY position
    `

	rows, err := r.client.QueryContext(ctx, query, database, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make(map[string]ColumnInfo)
	for rows.Next() {
		var name, colType string
		var comment sql.NullString

		if err := rows.Scan(&name, &colType, &comment); err != nil {
			return nil, err
		}

		columns[name] = ColumnInfo{
			Name:    name,
			Type:    colType,
			Comment: comment.String,
		}
	}

	return columns, rows.Err()
}

// getTableOrderBy retrieves the ORDER BY clause from ClickHouse
func (r *TableResource) getTableOrderBy(ctx context.Context, database, tableName string) ([]string, error) {
	query := `
        SELECT sorting_key
        FROM system.tables
        WHERE database = ? AND name = ?
    `

	var sortingKey sql.NullString
	err := r.client.QueryRowContext(ctx, query, database, tableName).Scan(&sortingKey)
	if err != nil {
		return nil, err
	}

	if !sortingKey.Valid || sortingKey.String == "" {
		return []string{}, nil
	}

	// Parse the sorting key (remove parentheses and split by comma)
	orderBy := strings.Trim(sortingKey.String, "()")
	if orderBy == "" {
		return []string{}, nil
	}

	columns := strings.Split(orderBy, ",")
	for i, col := range columns {
		columns[i] = strings.TrimSpace(col)
	}

	return columns, nil
}

// validateColumns compares expected vs actual columns
func (r *TableResource) validateColumns(expectedCols []ColumnModel, actualCols map[string]ColumnInfo) error {
	// Check if we have the right number of columns
	if len(expectedCols) != len(actualCols) {
		return fmt.Errorf("expected %d columns, found %d columns", len(expectedCols), len(actualCols))
	}

	// Check each expected column
	for _, expected := range expectedCols {
		actual, exists := actualCols[expected.Name.ValueString()]
		if !exists {
			return fmt.Errorf("column '%s' not found in table", expected.Name.ValueString())
		}

		// Validate column type
		if actual.Type != expected.Type.ValueString() {
			return fmt.Errorf("column '%s': expected type '%s', found type '%s'",
				expected.Name.ValueString(), expected.Type.ValueString(), actual.Type)
		}

		// Validate comment if specified
		expectedComment := ""
		if !expected.Comment.IsNull() && !expected.Comment.IsUnknown() {
			expectedComment = expected.Comment.ValueString()
		}

		if actual.Comment != expectedComment {
			return fmt.Errorf("column '%s': expected comment '%s', found comment '%s'",
				expected.Name.ValueString(), expectedComment, actual.Comment)
		}
	}

	return nil
}

// validateOrderBy compares expected vs actual ORDER BY clauses
func (r *TableResource) validateOrderBy(expected []types.String, actual []string) error {
	expectedStrs := make([]string, len(expected))
	for i, e := range expected {
		expectedStrs[i] = e.ValueString()
	}

	if len(expectedStrs) != len(actual) {
		return fmt.Errorf("expected ORDER BY with %d columns, found %d columns",
			len(expectedStrs), len(actual))
	}

	for i, expectedCol := range expectedStrs {
		if expectedCol != actual[i] {
			return fmt.Errorf("ORDER BY column %d: expected '%s', found '%s'",
				i+1, expectedCol, actual[i])
		}
	}

	return nil
}

// isMergeTreeFamily checks if the engine is part of MergeTree family
func (r *TableResource) isMergeTreeFamily(engine string) bool {
	mergeTreeEngines := []string{
		"MergeTree", "ReplacingMergeTree", "SummingMergeTree",
		"AggregatingMergeTree", "CollapsingMergeTree", "VersionedCollapsingMergeTree",
		"GraphiteMergeTree",
	}

	for _, mt := range mergeTreeEngines {
		if strings.HasPrefix(engine, mt) {
			return true
		}
	}
	return false
}

// ColumnInfo represents actual column information from ClickHouse
type ColumnInfo struct {
	Name    string
	Type    string
	Comment string
}
