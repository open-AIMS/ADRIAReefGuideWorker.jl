using ADRIA

"""
Model parameter definition matching the TypeScript schema
"""
struct ModelParam
    param_name::String
    third_param_flag::Bool
    lower::Float64
    upper::Float64
    optional_third::OptionalValue{Float64}
end

"""
    model_param_to_tuple(model_param::ModelParam)::Tuple

Convert the model parameter struct a tuple the can be passed to the ADRIA set_factor_bounds
function.
"""
function model_param_to_tuple(model_param::ModelParam)::Tuple
    if model_param.third_param_flag
        return (model_param.lower, model_param.upper, model_param.optional_third)
    else
        return (model_param.lower, model_param.upper)
    end
end

function update_domain_with_param!(; domain, param::ModelParam)
    # Set the distribution parametrisation defined by the user.
    return ADRIA.set_factor_bounds(
        Ref(domain),
        param.param_name,
        model_param_to_tuple(param)
    )
end

function update_domain_with_params!(; domain, params::Vector{ModelParam})
    for param::ModelParam in params
        @debug "Setting parameter:" param.param_name param.lower param.upper
        update_domain_with_param(;
            domain, param
        )
    end
end

"""
Move ADRIA result set from output directory to target location with specified name.

Reads ADRIA_OUTPUT_DIR environment variable, validates there's exactly one folder,
then renames and moves it to the target location.

# Arguments
- `target_location::String`: Directory where the result folder should be moved
- `folder_name::String`: New name for the result folder

# Throws
- `ErrorException`: If ADRIA_OUTPUT_DIR is not set
- `ErrorException`: If output directory doesn't exist
- `ErrorException`: If zero or multiple folders found in output directory
- `SystemError`: If file operations fail
"""
function move_result_set_to_determined_location(;
    target_location::String, folder_name::String
)
    @debug "Starting move_result_set_to_determined_location" target_location folder_name

    # Read environment variable
    output_dir = get(ENV, "ADRIA_OUTPUT_DIR", nothing)
    if output_dir === nothing
        error("ADRIA_OUTPUT_DIR environment variable is not set")
    end

    @debug "ADRIA_OUTPUT_DIR found" output_dir

    # Check if output directory exists
    if !isdir(output_dir)
        error("ADRIA_OUTPUT_DIR directory does not exist: $output_dir")
    end

    @debug "Output directory exists, listing contents"

    # List all directories in the output directory
    all_items = readdir(output_dir)
    @debug "Found $(length(all_items)) items in output directory" items = all_items

    # Filter to only directories (exclude files)
    directories = filter(item -> isdir(joinpath(output_dir, item)), all_items)
    @debug "Found $(length(directories)) directories" directories

    # Validate exactly one directory
    if length(directories) == 0
        error("No directories found in ADRIA_OUTPUT_DIR: $output_dir")
    elseif length(directories) > 1
        error(
            "Multiple directories found in ADRIA_OUTPUT_DIR (expected exactly 1): $directories"
        )
    end

    # Get the single directory name
    source_folder_name = directories[1]
    source_path = joinpath(output_dir, source_folder_name)
    @info "Found single result directory" source_folder = source_folder_name source_path

    # Ensure target location directory exists
    if !isdir(target_location)
        @debug "Creating target directory" target_location
        mkpath(target_location)
    end

    # Construct target path
    target_path = joinpath(target_location, folder_name)
    @debug "Target path determined" target_path

    # Check if target already exists
    if ispath(target_path)
        @warn "Target path already exists, will overwrite" target_path
        # Remove existing target to allow clean move
        rm(target_path; recursive=true, force=true)
        @debug "Removed existing target path"
    end

    # Move (rename) the directory
    @info "Moving result set" from = source_path to = target_path
    try
        mv(source_path, target_path)
        @info "Successfully moved result set" original_name = source_folder_name new_location =
            target_path
    catch e
        @error "Failed to move result set" exception = (e, catch_backtrace())
        rethrow(e)
    end

    # Verify the move was successful
    if !isdir(target_path)
        error(
            "Move operation appeared to succeed but target directory does not exist: $target_path"
        )
    end

    if isdir(source_path)
        error(
            "Move operation appeared to succeed but source directory still exists: $source_path"
        )
    end

    @info "Result set successfully moved and validated" target_path

    return target_path
end
