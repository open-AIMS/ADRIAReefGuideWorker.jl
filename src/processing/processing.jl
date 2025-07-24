using ADRIA
using JSON
using DataFrames
using ArchGDAL
using GeoDataFrames
using Parquet
using Logging
using Statistics

"""
    extract_location_scenario_data(result_set, scenarios_df; aggregations=[:mean])

Efficiently extract and aggregate timestep, scenario type, and relative cover data from ADRIA results.
Aggregates by scenario_type instead of individual scenarios.

# Arguments
- `result_set`: ADRIA ResultSet containing the relative_cover metric
- `scenarios_df`: DataFrame containing scenario specifications
- `aggregations`: Vector of aggregation functions to apply (default: [:mean, :min, :max])

# Returns
DataFrame with columns: timestep, scenario_type, relative_cover_mean, relative_cover_min, relative_cover_max, location_id
"""
function extract_location_scenario_data(
    result_set, scenarios_df
)
    # Get the relative cover data (should be 3D: timesteps × locations × scenarios)
    relative_cover_data = ADRIA.relative_cover(result_set)
    # Get the scenario groupings
    scenario_groups = ADRIA.analysis.scenario_types(scenarios_df)

    function scenario_id_to_type(scenario_id::Int)
        if scenario_groups[:guided][scenario_id] == 1
            return "guided"
        elseif scenario_groups[:unguided][scenario_id] == 1
            return "unguided"
        elseif scenario_groups[:counterfactual][scenario_id] == 1
            return "counterfactual"
        end
        return "unknown"
    end

    # Map to a dataframe by stacking the relative cover data
    df = DataFrame(stack(relative_cover_data))

    # Map the scenario ID in to the scenario type column
    df.scenario_types = scenario_id_to_type.(df.scenarios)

    # Now groupby the scenario_type and provide multiple aggregation columns
    # TODO consider other aggregations of interest
    aggregated = combine(groupby(df, [:scenario_types, :timesteps, :locations]),
        :value => mean => :relative_cover_mean,
        :value => minimum => :relative_cover_min,
        :value => maximum => :relative_cover_max
    )

    # update types due to poor inference from the relative cover metric
    transform!(aggregated,
        :timesteps => ByRow(Int) => :timesteps,
        :locations => ByRow(String) => :locations
    )

    return aggregated
end

"""
    load_spatial_data_from_datapackage(datapackage_path)

Load spatial data (geopackage) from an ADRIA data package.

# Arguments
- `datapackage_path`: Path to the datapackage.json file OR path to the domain directory containing datapackage.json

# Returns
GeoDataFrame containing the spatial data with geometry and reef/site information
"""
function load_spatial_data_from_datapackage(datapackage_path)
    # Handle case where user provides directory vs direct file path
    if isdir(datapackage_path)
        json_path = joinpath(datapackage_path, "datapackage.json")
        base_dir = datapackage_path
    elseif endswith(datapackage_path, "datapackage.json")
        json_path = datapackage_path
        base_dir = dirname(datapackage_path)
    else
        throw(
            ArgumentError(
                "datapackage_path must be either a directory containing datapackage.json or the path to datapackage.json itself"
            )
        )
    end

    # Check if datapackage.json exists
    if !isfile(json_path)
        throw(ArgumentError("datapackage.json not found at: $json_path"))
    end

    # Read and parse the JSON
    datapackage_content = JSON.parsefile(json_path)

    # Find the spatial data resource
    spatial_resource = nothing
    if haskey(datapackage_content, "resources")
        for resource in datapackage_content["resources"]
            if haskey(resource, "name") && resource["name"] == "spatial_data"
                spatial_resource = resource
                break
            end
        end
    end

    if spatial_resource === nothing
        throw(ArgumentError("No spatial_data resource found in datapackage.json"))
    end

    # Get the path to the geopackage
    if !haskey(spatial_resource, "path")
        throw(ArgumentError("spatial_data resource does not have a 'path' field"))
    end

    geopackage_path = joinpath(base_dir, spatial_resource["path"])

    if !isfile(geopackage_path)
        throw(ArgumentError("Geopackage file not found at: $geopackage_path"))
    end

    @debug "Loading spatial data from: $geopackage_path"

    # Load the geopackage using GeoDataFrames
    try
        gdf = GeoDataFrames.read(geopackage_path)
        @debug "Successfully loaded $(nrow(gdf)) spatial features"
        return gdf
    catch e
        throw(ArgumentError("Failed to load geopackage: $e"))
    end
end

"""
    create_location_geometry_lookup(spatial_gdf)

Create a lookup DataFrame mapping location_id to geometry for joining with ADRIA results.

# Arguments
- `spatial_gdf`: GeoDataFrame from load_spatial_data_from_datapackage()

# Returns
DataFrame with columns: location_id, geometry (and any other relevant spatial attributes)
"""
function create_location_geometry_lookup(spatial_gdf)
    @debug "Available columns in spatial data: $(names(spatial_gdf))"

    # Try to identify the location ID column
    location_id_col = nothing
    if "reef_siteid" in names(spatial_gdf)
        location_id_col = "reef_siteid"
        @debug "Using 'reef_siteid' as location identifier"
    elseif "site_id" in names(spatial_gdf)
        location_id_col = "site_id"
        @debug "Using 'site_id' as location identifier"
    else
        throw(
            ArgumentError(
                "Could not find 'reef_siteid' or 'site_id' column. Available columns: $(join(names(spatial_gdf), ", "))"
            )
        )
    end

    # Create lookup table
    lookup_df = DataFrames.select(spatial_gdf, location_id_col => :location_id, :geometry)

    # Add any other potentially useful columns
    other_useful_cols = ["zone_type", "area", "depth"] # common columns that might be useful
    for col in other_useful_cols
        if col in names(spatial_gdf)
            lookup_df[!, col] = spatial_gdf[!, col]
            @debug "Added column: $col"
        end
    end

    @debug "Created lookup table with $(nrow(lookup_df)) locations"
    return lookup_df
end

"""
    export_adria_web_data(result_set, scenarios_df, datapackage_path; 
                         geojson_path="spatial_data.geojson", 
                         parquet_path="relative_cover_data.parquet")

Complete workflow to process ADRIA results and export web-ready files.

# Arguments
- `result_set`: ADRIA ResultSet containing model results
- `scenarios_df`: DataFrame containing scenario specifications  
- `datapackage_path`: Path to ADRIA data package directory or datapackage.json

# Keyword Arguments
- `geojson_path`: Output path for spatial data GeoJSON file
- `parquet_path`: Output path for relative cover data Parquet file

# Returns
Dict with keys "spatial" and "data" pointing to the created file paths

# Example
```julia
files = export_adria_web_data(
    result, 
    scenarios, 
    "../data/Moore_2025-01-17_v070_rc1",
    geojson_path="outputs/moore_spatial.geojson",
    parquet_path="outputs/moore_data.parquet"
)
# Returns: Dict("spatial" => "outputs/moore_spatial.geojson", "data" => "outputs/moore_data.parquet")
```
"""
function export_adria_web_data(result_set, scenarios_df, datapackage_path;
    geojson_path="spatial_data.geojson",
    parquet_path="relative_cover_data.parquet")
    @info "Starting ADRIA web data export workflow"
    start_time = time()

    try
        # Step 1: Convert to tidy format for web visualization
        @info "Converting to tidy data format..."
        step_start = time()
        tidy_data = extract_location_scenario_data(result_set, scenarios_df)
        step_duration = time() - step_start
        @info "✓ Tidy data created in $(round(step_duration, digits=2)) seconds" nrows = nrow(
            tidy_data
        ) ncols = ncol(tidy_data)

        # Step 2: Load spatial/geometry data from the data package
        @info "Loading spatial data..."
        step_start = time()
        spatial_data = load_spatial_data_from_datapackage(datapackage_path)
        step_duration = time() - step_start
        @info "✓ Spatial data loaded in $(round(step_duration, digits=2)) seconds" nfeatures = nrow(
            spatial_data
        )

        # Step 3: Create geometry lookup table
        @info "Creating geometry lookup table..."
        step_start = time()
        geometry_lookup = create_location_geometry_lookup(spatial_data)
        step_duration = time() - step_start
        @info "✓ Geometry lookup created in $(round(step_duration, digits=2)) seconds" nlookups = nrow(
            geometry_lookup
        )

        # Step 4: Export spatial data
        @info "Exporting spatial data to GeoJSON..."
        step_start = time()
        mkpath(dirname(geojson_path))
        GeoDataFrames.write(geojson_path, geometry_lookup)
        step_duration = time() - step_start
        spatial_size_mb = round(stat(geojson_path).size / 1024^2; digits=2)
        @info "✓ Spatial data exported in $(round(step_duration, digits=2)) seconds" path =
            geojson_path size_mb = spatial_size_mb

        # Step 5: Export data table
        @info "Exporting data table to Parquet..."
        step_start = time()
        mkpath(dirname(parquet_path))
        Parquet.write_parquet(parquet_path, tidy_data)
        step_duration = time() - step_start
        data_size_mb = round(stat(parquet_path).size / 1024^2; digits=2)
        @info "✓ Data table exported in $(round(step_duration, digits=2)) seconds" path =
            parquet_path size_mb = data_size_mb

        # Summary
        total_duration = time() - start_time
        @info "✓ Export workflow complete" total_time_seconds = round(
            total_duration; digits=1
        ) data_points = nrow(tidy_data) locations = length(unique(tidy_data.location_id))

        return Dict(
            "spatial" => geojson_path,
            "data" => parquet_path
        )

    catch e
        @error "Export workflow failed" exception = e
        rethrow(e)
    end
end
