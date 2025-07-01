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
# ============================================
# ADRIA_MODEL_RUN - this is an example set of job methods
# ============================================
#

"""
Input payload for ADRIA_MODEL_RUN job
"""
struct AdriaModelRunInput <: AbstractJobInput
    id::Int64
end

"""
Output payload for ADRIA_MODEL_RUN job
"""
struct AdriaModelRunOutput <: AbstractJobOutput
end

"""
Handler for ADRIA_MODEL_RUN jobs
"""
struct AdriaModelRunHandler <: AbstractJobHandler end

"""
Process a ADRIA_MODEL_RUN job
"""
function handle_job(
    ::AdriaModelRunHandler, input::AdriaModelRunInput, context::HandlerContext
)::AdriaModelRunOutput
    @debug "Processing test job with id: $(input.id)"

    # Define the domain data path
    data_pkg_path = "../data/Moore_2025-01-17_v070_rc1"
    # load the domain (RCP 4.5)
    domain = ADRIA.load_domain(data_pkg_path, "45")
    # generate scenarios (must be power of 2)
    scenarios = ADRIA.sample(domain, 128)
    # run generated scenarios for RCP 4.5
    result = ADRIA.run_scenarios(domain, scenarios, "45")
    # generate a figure from the result
    relative_cover = ADRIA.metrics.scenario_relative_cover(result)
    fig = ADRIA.viz.scenarios(result, relative_cover)
    save("../data/output.png", fig)

    @debug "Finished test job with id: $(input.id)"
    @debug "Could write something to $(context.storage_uri) if desired."

    # This is where the actual job processing would happen
    # For now, we just return a dummy output
    return AdriaModelRunOutput()
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

    @debug "Jobs module initialized with handlers"
end
