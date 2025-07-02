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
    return ADRIA.set_factor_bounds!(
        domain,
        Symbol(param.param_name),
        model_param_to_tuple(param)
    )
end

function update_domain_with_params!(; domain, params::Vector{ModelParam})
    for param::ModelParam in params
        @debug "Setting parameter:" param.param_name param.lower param.upper
        update_domain_with_param!(;
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

"""
    create_unique_folder(; base_dir::String, prefix::String="folder", suffix::String="")

Create a unique folder name within the specified base directory.

# Arguments
- `base_dir::String`: Base directory where the folder will be created
- `prefix::String="folder"`: Prefix for the folder name (default: "folder")  
- `suffix::String=""`: Optional suffix for the folder name

# Returns
- `String`: Full path to the unique folder

# Example
```julia
path = create_unique_folder(base_dir="/tmp", prefix="data", suffix="_backup")
# Returns something like: "/tmp/data_20240702_143052_abc123_backup"
```
"""
function create_unique_folder(;
    base_dir::String, prefix::String="folder", suffix::String=""
)
    # Ensure base directory exists
    if !isdir(base_dir)
        @info "Base directory does not exist, creating"
        mkdir(base_dir)
    end

    # Generate unique identifier using timestamp and random string
    timestamp = Dates.format(now(), "yyyymmdd_HHMMSS")
    random_id = randstring(6)

    # Construct folder name
    folder_name = "$(prefix)_$(timestamp)_$(random_id)$(suffix)"
    full_path = joinpath(base_dir, folder_name)

    # Handle extremely unlikely collision
    counter = 1
    while isdir(full_path)
        folder_name = "$(prefix)_$(timestamp)_$(random_id)_$(counter)$(suffix)"
        full_path = joinpath(base_dir, folder_name)
        counter += 1
    end

    # create the path 
    mkpath(full_path)
    return full_path
end

"""
    set_adria_output_dir(unique_path::String)

Update the ADRIA_OUTPUT_DIR environment variable to the specified path.

# Arguments
- `unique_path::String`: The new path to set for ADRIA_OUTPUT_DIR

# Example
```julia
unique_path = create_unique_folder(base_dir="../data/outputs", prefix="run")
set_adria_output_dir(unique_path)
```
"""
function set_adria_output_dir(path::String)
    ENV["ADRIA_OUTPUT_DIR"] = path
    @info "Updated ADRIA_OUTPUT_DIR to: $path"
end

"""
    rmdir(dir_path::String; verbose::Bool=true)

Safely remove a directory and all its contents programmatically.

# Arguments
- `dir_path::String`: Path to the directory to remove
- `verbose::Bool=true`: If true, log deletion progress

# Safety Features
- Validates directory exists before attempting deletion
- Prevents deletion of critical system directories (/, /home, /usr, etc.)
- Handles permission errors gracefully

# Returns
- `Bool`: true if deletion successful, false otherwise

# Example
```julia
success = rmdir("/tmp/my_temp_folder")
rmdir("/path/to/folder", verbose=false)
```
"""
function rmdir(dir_path::String; verbose::Bool=true)
    # Normalize path to get absolute path
    normalized_path = abspath(dir_path)

    # Check if directory exists
    if !isdir(normalized_path)
        verbose && @warn "Directory does not exist: $normalized_path"
        return false
    end

    # Safety check: prevent deletion of critical system directories
    # Check for exact matches only, not path prefixes
    dangerous_paths = [
        "/", "/home", "/usr", "/bin", "/sbin", "/etc", "/var", "/boot", "/opt"
    ]
    if normalized_path in dangerous_paths
        @error "Refusing to delete system directory: $normalized_path"
        return false
    end

    # Attempt deletion
    try
        verbose && @info "Removing directory: $normalized_path"
        rm(normalized_path; recursive=true, force=true)
        verbose && @info "Successfully removed: $normalized_path"
        return true
    catch e
        @error "Failed to remove directory: $e"
        return false
    end
end
