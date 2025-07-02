using ADRIA
using VegaLite
using DataFrames

"""
    extract_scenario_data(metric_array, scenarios_df)

Extract and categorize scenario data from ADRIA metrics for visualization.

# Arguments
- `metric_array`: 2D YAXArray with dimensions (timesteps, scenarios)
- `scenarios_df`: DataFrame containing scenario specifications

# Returns
DataFrame with columns: timestep, scenario, scenario_type, value
"""
function extract_scenario_data(metric_array, scenarios_df)
    # Get dimensions
    timesteps = collect(metric_array.timesteps)
    scenario_indices = collect(metric_array.scenarios)

    # Get ADRIA scenario type groupings
    scen_groups = ADRIA.analysis.scenario_types(scenarios_df)

    # Create scenario type mapping
    scenario_type_map = Dict{Int,String}()
    for (type_name, mask) in scen_groups
        scenario_indices_for_type = findall(mask)
        for idx in scenario_indices_for_type
            scenario_type_map[idx] = string(type_name)
        end
    end

    # Build DataFrame
    data_rows = []
    for (i, timestep) in enumerate(timesteps)
        for (j, scenario_idx) in enumerate(scenario_indices)
            push!(
                data_rows,
                (
                    timestep=timestep,
                    scenario=scenario_idx,
                    scenario_type=get(scenario_type_map, scenario_idx, "unknown"),
                    value=metric_array[i, j]
                )
            )
        end
    end

    return DataFrame(data_rows)
end

"""
    create_scenario_plot_spec(; 
        title="Scenario Analysis",
        x_label="Year", 
        y_label="Value",
        plot_style=:confidence_bands,
        colors=Dict("counterfactual" => "#d62728", "unguided" => "#2ca02c", "guided" => "#1f77b4"),
        width=700,
        height=400
    )

Create a VegaLite specification for ADRIA scenario visualization.

# Keyword Arguments
- `title`: Plot title
- `x_label`: X-axis label  
- `y_label`: Y-axis label
- `plot_style`: Either `:confidence_bands` or `:individual_lines`
- `colors`: Dict mapping scenario types to colors
- `width`, `height`: Plot dimensions

# Returns
VegaLite specification string
"""
function create_scenario_plot_spec(;
    title="Scenario Analysis",
    x_label="Year",
    y_label="Value",
    plot_style=:confidence_bands,
    colors=Dict(
        "counterfactual" => "#d62728", "unguided" => "#2ca02c", "guided" => "#1f77b4"
    ),
    width=700,
    height=400
)

    # Common encodings - always use "value" field
    base_encodings = Dict(
        "x" => Dict(
            "field" => "timestep",
            "type" => "ordinal",
            "axis" => Dict(
                "title" => x_label,
                "labelAngle" => 0,
                "labelOverlap" => "parity",
                "labelFontSize" => 10,
                "titleFontSize" => 12
            )
        ),
        "y" => Dict(
            "field" => "value",
            "type" => "quantitative",
            "axis" => Dict(
                "title" => y_label,
                "titleFontSize" => 12,
                "labelFontSize" => 10,
                "grid" => true
            ),
            "scale" => Dict("zero" => false)
        ),
        "color" => Dict(
            "field" => "scenario_type",
            "type" => "nominal",
            "scale" => Dict(
                "domain" => collect(keys(colors)),
                "range" => collect(values(colors))
            ),
            "legend" => Dict(
                "title" => "Scenario Type",
                "titleFontSize" => 12,
                "labelFontSize" => 10,
                "symbolSize" => 100
            )
        )
    )

    # Create layers based on plot style
    layers = if plot_style == :confidence_bands
        [
            # Confidence bands
            Dict(
                "mark" => Dict(
                    "type" => "errorband",
                    "extent" => "ci",
                    "opacity" => 0.4
                ),
                "encoding" => base_encodings
            ),
            # Mean lines
            Dict(
                "mark" => Dict(
                    "type" => "line",
                    "strokeWidth" => 2
                ),
                "encoding" => merge(
                    base_encodings,
                    Dict(
                        "y" => merge(base_encodings["y"], Dict(
                            "aggregate" => "mean"
                        ))
                    )
                )
            )
        ]
    elseif plot_style == :individual_lines
        [
            Dict(
                "mark" => Dict(
                    "type" => "line",
                    "strokeWidth" => 1,
                    "opacity" => 0.6
                ),
                "encoding" => merge(
                    base_encodings,
                    Dict(
                        "detail" => Dict("field" => "scenario", "type" => "nominal")
                    )
                )
            )
        ]
    else
        throw(ArgumentError("plot_style must be :confidence_bands or :individual_lines"))
    end

    # Build complete spec
    spec = Dict(
        "\$schema" => "https://vega.github.io/schema/vega-lite/v6.json",
        "title" => Dict(
            "text" => title,
            "fontSize" => 16,
            "anchor" => "start"
        ),
        "width" => width,
        "height" => height,
        "layer" => layers
    )

    return spec
end

"""
    plot_adria_scenarios(scenarios_df, result_set, metric_array; 
        title="Scenario Analysis",
        y_label="Value",
        plot_style=:confidence_bands,
        kwargs...)

Create a VegaLite plot for ADRIA scenario analysis.

# Arguments
- `scenarios_df`: DataFrame containing scenario specifications
- `result_set`: ADRIA ResultSet (used for automatic labeling)
- `metric_array`: 2D YAXArray with dimensions (timesteps, scenarios)

# Keyword Arguments
- `title`: Plot title
- `y_label`: Y-axis label
- `plot_style`: Either `:confidence_bands` or `:individual_lines`
- Additional kwargs passed to `create_scenario_plot_spec`

# Returns
VegaLite plot object ready to be saved

# Examples
```julia
# Basic usage
plot = plot_adria_scenarios(scenarios, result, relative_cover, 
                           title="Coral Cover Analysis",
                           y_label="Relative Cover")
save("coral_cover.png", plot)

# Different metric
juveniles = ADRIA.metrics.scenario_relative_juveniles(result)
plot = plot_adria_scenarios(scenarios, result, juveniles,
                           title="Juvenile Population",
                           y_label="Relative Juveniles")
"""
function plot_adria_scenarios(scenarios_df, result_set, metric_array;
    title="Scenario Analysis",
    y_label="Value",
    plot_style=:confidence_bands,
    kwargs...)

    # Extract and process data (always returns "value" column)
    df = extract_scenario_data(metric_array, scenarios_df)

    # Create plot specification
    spec_dict = create_scenario_plot_spec(;
        title=title,
        y_label=y_label,
        plot_style=plot_style,
        kwargs...
    )

    # Convert to VegaLite spec and apply data
    vl_spec = VegaLite.VLSpec(spec_dict)
    plot = vl_spec(df)

    return plot
end
