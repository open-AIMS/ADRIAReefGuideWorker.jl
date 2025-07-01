"""
This is the file where handlers, input and output payloads are registered to
handle jobs for this worker.
"""

using ADRIA
using CairoMakie, GeoMakie, GraphMakie

# ================
# Type Definitions
# ================

"""
Enum for job types matching the API definition
"""
@enum JobType begin
    ADRIA_MODEL_RUN
end

symbol_to_job_type = Dict(zip(Symbol.(instances(JobType)), instances(JobType)))

"""
Enum for storage schemes matching the API definition
"""
@enum StorageScheme begin
    S3
    # Add more storage schemes as needed
end

"""
Abstract type for job input payloads
All concrete job input types should inherit from this
"""
abstract type AbstractJobInput end

"""
Abstract type for job output payloads
All concrete job output types should inherit from this
"""
abstract type AbstractJobOutput end

"""
Abstract type for job handler implementations
All concrete job handlers should inherit from this
"""
abstract type AbstractJobHandler end

"""
A context object passed through to a job handler
"""
struct HandlerContext
    "The path to the s3 storage location permitted for writing"
    storage_uri::String
    aws_region::String
    s3_endpoint::OptionalValue{String}

    function HandlerContext(;
        storage_uri::String, aws_region::String="ap-southeast-2",
        s3_endpoint::OptionalValue{String}=nothing
    )
        return new(storage_uri, aws_region, s3_endpoint)
    end
end

"""
Registry mapping job types to handlers, input/output types, and validators
"""
struct JobRegistry
    handlers::Dict{JobType,AbstractJobHandler}
    input_types::Dict{JobType,Type{<:AbstractJobInput}}
    output_types::Dict{JobType,Type{<:AbstractJobOutput}}

    function JobRegistry()
        return new(
            Dict{JobType,AbstractJobHandler}(),
            Dict{JobType,Type{<:AbstractJobInput}}(),
            Dict{JobType,Type{<:AbstractJobOutput}}()
        )
    end
end

# Global registry instance
const JOB_REGISTRY = JobRegistry()

# ======================
# Registration functions
# ======================

"""
Register a job handler for a specific job type
"""
function register_job_handler!(
    job_type::JobType,
    handler::AbstractJobHandler,
    input_type::Type{<:AbstractJobInput},
    output_type::Type{<:AbstractJobOutput}
)
    JOB_REGISTRY.handlers[job_type] = handler
    JOB_REGISTRY.input_types[job_type] = input_type
    JOB_REGISTRY.output_types[job_type] = output_type

    @debug "Registered handler for job type: $job_type"
    return nothing
end

"""
Get the appropriate handler for a job type
"""
function get_job_handler(job_type::JobType)::AbstractJobHandler
    if !haskey(JOB_REGISTRY.handlers, job_type)
        error("No handler registered for job type: $job_type")
    end
    return JOB_REGISTRY.handlers[job_type]
end

#
# Validation functions
#

"""
Parse and validate a job input payload
"""
function validate_job_input(job_type::JobType, raw_payload::Any)
    if !haskey(JOB_REGISTRY.input_types, job_type)
        error("No input type registered for job type: $job_type")
    end

    input_type = JOB_REGISTRY.input_types[job_type]

    try
        # Parse the raw JSON payload into the appropriate type
        return JSON3.read(JSON3.write(raw_payload), input_type)
    catch e
        @error "Input validation failed for job type $job_type" exception = (
            e, catch_backtrace()
        )
        error("Invalid input payload for job type: $job_type")
    end
end

"""
Validate a job output payload
"""
function validate_job_output(job_type::JobType, output::AbstractJobOutput)
    if !haskey(JOB_REGISTRY.output_types, job_type)
        error("No output type registered for job type: $job_type")
    end

    expected_type = JOB_REGISTRY.output_types[job_type]

    if !isa(output, expected_type)
        error("Output payload is not of the correct type for job type: $job_type")
    end

    return output
end

#
# Job processing
#

"""
Process a job using the appropriate handler
"""
function process_job(
    job_type::JobType, input_payload::Any, context::HandlerContext
)::AbstractJobOutput
    # Get the registered handler
    handler = get_job_handler(job_type)

    # Validate and convert input payload
    typed_input = validate_job_input(job_type, input_payload)

    # Process the job
    @debug "Processing job of type: $job_type"
    output = handle_job(handler, typed_input, context)

    # Validate output
    validate_job_output(job_type, output)

    return output
end

#
# =====================================================
# ADRIA_MODEL_RUN - Model parameter and job definitions
# =====================================================
#

"""
Input payload for ADRIA_MODEL_RUN job
"""
struct AdriaModelRunInput <: AbstractJobInput
    num_scenarios::Int64
    model_params::Vector{ModelParam}
    rcp_scenario::OptionalValue{String}  # defaults to "45" if not provided
end

"""
Output payload for ADRIA_MODEL_RUN job
"""
struct AdriaModelRunOutput <: AbstractJobOutput
    # Relative S3 storage location of result set
    output_result_set_path::String
    # Relative S3 storage location of figure (png)
    output_figure_path::String
end

"""
Handler for ADRIA_MODEL_RUN jobs
"""
struct AdriaModelRunHandler <: AbstractJobHandler end

"""
Process an ADRIA_MODEL_RUN job
"""
function handle_job(
    ::AdriaModelRunHandler, input::AdriaModelRunInput, context::HandlerContext
)::AdriaModelRunOutput
    @info "Starting ADRIA model run: $(input.run_name)"
    start_time = time()

    # Use provided RCP scenario or default to "45"
    rcp_scenario = something(input.rcp_scenario, "45")
    @info "Using RCP scenario: $rcp_scenario"

    # Define the domain data path
    # TODO (make configurable)
    data_pkg_path = "../data/Moore_2025-01-17_v070_rc1"

    # Load the domain
    @info "Loading domain data from: $data_pkg_path"
    domain = ADRIA.load_domain(data_pkg_path, rcp_scenario)

    # Apply custom parameters if provided
    if !isempty(input.model_params)
        @info "Applying $(length(input.model_params)) custom model parameters"
        update_domain_with_params!(; domain, params=input.model_params)
    end

    # Generate scenarios with the specified number
    @info "Generating $(input.num_scenarios) scenarios"
    scenarios = ADRIA.sample(domain, input.num_scenarios)

    # Run the scenarios
    @info "Running scenarios for RCP $rcp_scenario"
    result = ADRIA.run_scenarios(domain, scenarios, rcp_scenario)

    execution_time = time() - start_time
    @info "ADRIA model run completed in $(round(execution_time, digits=2)) seconds"

    # Move and rename the output to specified location
    # TODO this won't be necessary once we can 
    result_set_name = "result_set"
    output_dir = "../data/uploads"
    move_result_set_to_determined_location(;
        target_location=output_dir,
        folder_name=result_set_name
    )

    # generate a figure from the result
    figure_output_name_relative = "figure.png"
    relative_cover = ADRIA.metrics.scenario_relative_cover(result)
    fig = ADRIA.viz.scenarios(result, relative_cover)
    save(joinpath(output_dir, figure_output_name_relative), fig)

    # Now upload this to s3 
    client = S3StorageClient(; region=context.aws_region, s3_endpoint=context.s3_endpoint)

    # Output file names
    full_s3_target = "$(context.storage_uri)/$(result_set_name)"

    @debug now() "Initiating file upload of result set"
    upload_directory(client, joinpath(output_dir, result_set_name), full_s3_target)
    @debug now() "File upload completed"

    # Output file names
    full_s3_target = "$(context.storage_uri)/$(figure_output_name_relative)"

    @debug now() "Initiating file upload of figure"
    upload_directory(
        client, joinpath(output_dir, figure_output_name_relative), full_s3_target
    )
    @debug now() "File upload completed"

    # Need to upload!
    return AdriaModelRunOutput(result_set_name, figure_output_name_relative)
end

#
# ====
# INIT
# ====
#

#
# Register the job types when the module loads
#
function __init__()
    # Register the ADRIA_MODEL_RUN job handler
    register_job_handler!(
        ADRIA_MODEL_RUN,
        AdriaModelRunHandler(),
        AdriaModelRunInput,
        AdriaModelRunOutput
    )

    @debug "ADRIA Jobs module initialized with handlers"
end
