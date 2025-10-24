using ADRIA
using JSON
using DataFrames
using ArchGDAL
using GeoDataFrames
using Parquet
using Logging
using Statistics

# Define mode types for spatial data loading dispatch
abstract type SpatialDataMode end
struct DatapackageMode <: SpatialDataMode end
struct RMEMode <: SpatialDataMode end

# Convenience constants for mode selection
const DATAPACKAGE_MODE = DatapackageMode()
const RME_MODE = RMEMode()

"""
    extract_location_scenario_data(result_set)

Efficiently extract and aggregate timestep, scenario type, and relative cover data from ADRIA results.
Aggregates by scenario_type (guided, unguided, counterfactual) instead of individual scenarios.

# Arguments
- `result_set`: ADRIA ResultSet containing the relative_cover metric

# Returns
DataFrame with columns:
- `scenario_types`: Scenario type ("guided", "unguided", "counterfactual", or "unknown")
- `timesteps`: Time step as integer
- `locations`: Location ID as string
- `relative_cover_mean`: Mean relative cover across scenarios of the same type
- `relative_cover_min`: Minimum relative cover across scenarios of the same type
- `relative_cover_max`: Maximum relative cover across scenarios of the same type
- `relative_cover_ci_lower`: 2.5th percentile (lower confidence interval bound)
- `relative_cover_ci_upper`: 97.5th percentile (upper confidence interval bound)

# Example
```julia
tidy_data = extract_location_scenario_data(result_set)
```
"""
function extract_location_scenario_data(result_set)
    # @here
    # Extract 3D relative cover data (timesteps × locations × scenarios)
    relative_cover_data = ADRIA.relative_cover(result_set)

    # Get scenario type groupings from ADRIA analysis
    scenario_groups = ADRIA.analysis.scenario_types(result_set.inputs)

    """
    Map scenario ID to its corresponding scenario type.

    # Arguments
    - `scenario_id::Int`: Numeric scenario identifier

    # Returns
    String indicating scenario type: "guided", "unguided", "counterfactual", or "unknown"
    """
    function scenario_id_to_type(scenario_id::Int)
        for stype in ADRIA.analysis.SCENARIO_TYPES
            # Only handle relevant scenario types for provided result set
            if stype in keys(scenario_groups)
                if scenario_groups[stype][scenario_id] == 1
                    return String(stype)
                end
            end
        end

        return "unknown"
    end

    # TODO: Investigate use of ADRIA provided location-based metric
    # Suspect it will be much more performant than interacting with a DataFrame
    # ADRIA.metrics.loc_trajectory(mean, relative_cover_data)

    # Convert 3D array to long-format DataFrame using stack()
    df = DataFrame(stack(relative_cover_data))

    # Map scenario IDs to scenario types
    df.scenario_types = scenario_id_to_type.(df.scenarios)

    # Group by scenario type, timestep, and location, then aggregate
    aggregated = combine(groupby(df, [:scenario_types, :timesteps, :locations]),
        :value => mean => :relative_cover_mean,
        :value => minimum => :relative_cover_min,
        :value => maximum => :relative_cover_max,
        :value => (x -> quantile(x, 0.025)) => :relative_cover_ci_lower,
        :value => (x -> quantile(x, 0.975)) => :relative_cover_ci_upper
    )

    # Fix column types (stack() sometimes creates suboptimal types)
    transform!(aggregated,
        :timesteps => ByRow(Int) => :timesteps,
        :locations => ByRow(String) => :locations
    )

    return aggregated
end

"""
    load_spatial_data_from_datapackage(datapackage_path, mode=DATAPACKAGE_MODE)

Load spatial data (geopackage) using different modes for locating the spatial data file.

# Arguments
- `datapackage_path`: Base path for spatial data lookup (interpretation depends on mode)
- `mode`: Spatial data loading mode (DATAPACKAGE_MODE or RME_MODE)

# Modes
- `DATAPACKAGE_MODE`: Parse datapackage.json metadata to locate geopackage (default behavior)
- `RME_MODE`: Use fixed path structure `data_files/region/reefmod_gbr.gpkg` within the base path

# Returns
GeoDataFrame containing the spatial data with geometry and reef/site information

# Throws
- `ArgumentError`: If spatial data files are not found or cannot be loaded

# Examples
```julia
# Datapackage mode (default)
spatial_data = load_spatial_data_from_datapackage("../data/Moore_2025-01-17_v070_rc1/")
spatial_data = load_spatial_data_from_datapackage("../data/Moore_2025-01-17_v070_rc1/", DATAPACKAGE_MODE)

# RME mode
spatial_data = load_spatial_data_from_datapackage("../rme_data/", RME_MODE)
```
"""
function load_spatial_data_from_datapackage(
    datapackage_path, mode::SpatialDataMode=DATAPACKAGE_MODE
)
    return load_spatial_data_from_datapackage(datapackage_path, mode)
end

"""
    load_spatial_data_from_datapackage(datapackage_path, ::DatapackageMode)

DATAPACKAGE_MODE implementation: Load spatial data by parsing datapackage.json metadata.

# Arguments
- `datapackage_path`: Path to the datapackage.json file OR path to the domain directory containing datapackage.json
- `::DatapackageMode`: Mode dispatch parameter

# Returns
GeoDataFrame containing the spatial data with geometry and reef/site information
"""
function load_spatial_data_from_datapackage(datapackage_path, ::DatapackageMode)
    # Handle both directory and direct file path inputs
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

    # Verify datapackage.json exists
    if !isfile(json_path)
        throw(ArgumentError("datapackage.json not found at: $json_path"))
    end

    # Parse the datapackage metadata
    datapackage_content = JSON.parsefile(json_path)

    # Locate the spatial data resource in the package
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

    # Extract the geopackage file path
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
    load_spatial_data_from_datapackage(base_path, ::RMEMode)

RME_MODE implementation: Load spatial data using fixed ReefMod-E path structure.

# Arguments
- `base_path`: Base directory containing the RME data structure
- `::RMEMode`: Mode dispatch parameter

# Returns
GeoDataFrame containing the spatial data with geometry and reef/site information

# Notes
Expects geopackage at: `{base_path}/data_files/region/reefmod_gbr.gpkg`
"""
function load_spatial_data_from_datapackage(base_path, ::RMEMode)
    # Construct the fixed RME path structure
    geopackage_path = joinpath(base_path, "data_files", "region", "reefmod_gbr.gpkg")

    @debug "RME Mode: Looking for spatial data at: $geopackage_path"

    if !isfile(geopackage_path)
        throw(
            ArgumentError(
                "RME geopackage file not found at expected path: $geopackage_path"
            )
        )
    end

    # Load the geopackage using GeoDataFrames
    try
        gdf = GeoDataFrames.read(geopackage_path)
        @debug "Successfully loaded $(nrow(gdf)) spatial features from RME structure"
        return gdf
    catch e
        throw(ArgumentError("Failed to load RME geopackage: $e"))
    end
end

"""
    create_location_geometry_lookup(spatial_gdf)

Create a lookup DataFrame mapping location_id to geometry for joining with ADRIA results.
Automatically detects the appropriate location ID column and includes useful spatial attributes.

# Arguments
- `spatial_gdf`: GeoDataFrame from `load_spatial_data_from_datapackage()`

# Returns
DataFrame with columns:
- `location_id`: Location identifier (mapped from reef_siteid, site_id, or RME_UNIQUE_ID)
- `geometry`: Spatial geometry for the location
- Additional columns if present: `zone_type`, `area`, `depth`

# Throws
- `ArgumentError`: If none of the expected location ID columns are found

# Example
```julia
lookup_table = create_location_geometry_lookup(spatial_gdf)
```
"""
function create_location_geometry_lookup(spatial_gdf)
    @debug "Available columns in spatial data: $(names(spatial_gdf))"

    # Auto-detect the location ID column using common naming patterns
    # Order of preference: reef_siteid -> site_id -> RME_UNIQUE_ID
    location_id_col = nothing
    if "reef_siteid" in names(spatial_gdf)
        location_id_col = "reef_siteid"
        @debug "Using 'reef_siteid' as location identifier"
    elseif "site_id" in names(spatial_gdf)
        location_id_col = "site_id"
        @debug "Using 'site_id' as location identifier"
    elseif "RME_UNIQUE_ID" in names(spatial_gdf)
        location_id_col = "RME_UNIQUE_ID"
        @debug "Using 'RME_UNIQUE_ID' as location identifier"
    else
        throw(
            ArgumentError(
                "Could not find location ID column. Expected one of: 'reef_siteid', 'site_id', 'RME_UNIQUE_ID'. Available columns: $(join(names(spatial_gdf), ", "))"
            )
        )
    end

    # Create the core lookup table with location_id and geometry
    lookup_df = DataFrames.select(spatial_gdf, location_id_col => :location_id, :geometry)

    @debug "Created lookup table with $(nrow(lookup_df)) locations"
    return lookup_df
end
"""
    export_adria_web_data(result_set, scenarios_df, datapackage_path; 
                         mode=DATAPACKAGE_MODE,
                         geojson_path="spatial_data.geojson", 
                         parquet_path="relative_cover_data.parquet")

Complete workflow to process ADRIA results and export web-ready files for visualization.
This function combines all processing steps into a single, convenient interface.

# Arguments
- `result_set`: ADRIA ResultSet containing model results with relative_cover metric
- `scenarios_df`: DataFrame containing scenario specifications and type information
- `datapackage_path`: Path to data location (interpretation depends on mode)

# Keyword Arguments
- `mode::SpatialDataMode`: Spatial data loading mode (DATAPACKAGE_MODE or RME_MODE, default: DATAPACKAGE_MODE)
- `geojson_path::String`: Output path for spatial data GeoJSON file (default: "spatial_data.geojson")
- `parquet_path::String`: Output path for relative cover data Parquet file (default: "relative_cover_data.parquet")

# Returns
Dict{String, String} with keys:
- `"spatial"`: Path to the exported GeoJSON file
- `"data"`: Path to the exported Parquet file

# Process Overview
1. Converts ADRIA results to tidy format aggregated by scenario type
2. Loads spatial data using the specified mode
3. Creates geometry lookup table
4. Exports spatial data as GeoJSON
5. Exports aggregated data as Parquet

# Examples
```julia
# Using datapackage mode (default)
files = export_adria_web_data(
    result, 
    scenarios, 
    "../data/Moore_2025-01-17_v070_rc1",
    geojson_path="outputs/moore_spatial.geojson",
    parquet_path="outputs/moore_data.parquet"
)

# Using RME mode
files = export_adria_web_data(
    result, 
    scenarios, 
    "../rme_data/",
    mode=RME_MODE,
    geojson_path="outputs/rme_spatial.geojson",
    parquet_path="outputs/rme_data.parquet"
)
```

# Notes
- Automatically creates output directories if they don't exist
- Provides detailed logging of progress and file sizes
- All intermediate processing steps are logged with timing information
- Mode selection affects how spatial data is located and loaded
"""
function export_adria_web_data(result_set, scenarios_df, datapackage_path;
    mode::SpatialDataMode=DATAPACKAGE_MODE,
    geojson_path="spatial_data.geojson",
    parquet_path="relative_cover_data.parquet")
    @info "Starting ADRIA web data export workflow"
    start_time = time()

    try
        # Step 1: Convert ADRIA results to tidy format aggregated by scenario type
        @info "Converting to tidy data format..."
        step_start = time()
        tidy_data = extract_location_scenario_data(result_set)
        step_duration = time() - step_start
        @info "✓ Tidy data created in $(round(step_duration, digits=2)) seconds" nrows = nrow(
            tidy_data
        ) ncols = ncol(tidy_data)

        # Step 2: Load spatial/geometry data using the specified mode
        @info "Loading spatial data using $(typeof(mode)) mode..."
        step_start = time()
        spatial_data = load_spatial_data_from_datapackage(datapackage_path, mode)
        step_duration = time() - step_start
        @info "✓ Spatial data loaded in $(round(step_duration, digits=2)) seconds" nfeatures = nrow(
            spatial_data
        )

        # Step 3: Create geometry lookup table for web visualization
        @info "Creating geometry lookup table..."
        step_start = time()
        geometry_lookup = create_location_geometry_lookup(spatial_data)
        step_duration = time() - step_start
        @info "✓ Geometry lookup created in $(round(step_duration, digits=2)) seconds" nlookups = nrow(
            geometry_lookup
        )

        # Step 4: Export spatial data as GeoJSON for web mapping
        @info "Exporting spatial data to GeoJSON..."
        step_start = time()
        mkpath(dirname(geojson_path))  # Create output directory if needed
        GeoDataFrames.write(geojson_path, geometry_lookup)
        step_duration = time() - step_start
        spatial_size_mb = round(stat(geojson_path).size / 1024^2; digits=2)
        @info "✓ Spatial data exported in $(round(step_duration, digits=2)) seconds" path =
            geojson_path size_mb = spatial_size_mb

        # Step 5: Export aggregated data as Parquet for efficient web loading
        @info "Exporting data table to Parquet..."
        step_start = time()
        mkpath(dirname(parquet_path))  # Create output directory if needed
        Parquet.write_parquet(parquet_path, tidy_data)
        step_duration = time() - step_start
        data_size_mb = round(stat(parquet_path).size / 1024^2; digits=2)
        @info "✓ Data table exported in $(round(step_duration, digits=2)) seconds" path =
            parquet_path size_mb = data_size_mb

        # Workflow completion summary
        total_duration = time() - start_time
        @info "✓ Export workflow complete" total_time_seconds = round(
            total_duration; digits=1
        ) data_points = nrow(tidy_data) locations = length(unique(tidy_data.locations))

        return Dict(
            "spatial" => geojson_path,
            "data" => parquet_path
        )

    catch e
        @error "Export workflow failed" exception = e
        rethrow(e)
    end
end
