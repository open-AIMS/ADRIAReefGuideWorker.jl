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
Represents a job that needs to be processed
"""
struct Job
    "Job ID in the DB"
    id::Int64
    "What type of Job - strict set of enums"
    type::String
    "The payload defining the job parameters - should correspond to input payload type registered in Jobs.jl"
    input_payload::Any
end

"""
Represents an assignment for a job
"""
struct JobAssignment
    "ID of the assignment in the DB"
    id::Int64
    "ID of the tasked job"
    job_id::Int64
    "Path to where the data can be stored (in s3)"
    storage_uri::String
end

"""
Context provided to job handlers with all necessary information
"""
struct JobContext
    "Worker configuration"
    config::WorkerConfig
    "The job to be processed"
    job::Job
    "The job assignment details"
    assignment::JobAssignment
    "The API client for making HTTP requests"
    http_client::AuthApiClient
    "Storage client e.g. s3"
    storage_client::StorageClient
    "Task metadata"
    task_metadata::Any
    "AWS region for s3 storage"
    aws_region::String
    "S3 endpoint"
    s3_endpoint::OptionalValue{String}

    "Constructor that takes all fields"
    function JobContext(;
        config::WorkerConfig,
        job::Job,
        assignment::JobAssignment,
        http_client::AuthApiClient,
        storage_client::StorageClient,
        task_metadata::Any
    )
        return new(
            config,
            job,
            assignment,
            http_client,
            storage_client,
            task_metadata
        )
    end
end

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
    job_type::JobType, input_payload::Any, context::JobContext
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
    # Needs to be one of the available data packages (currently MOORE or GBR)
    data_package::String
    num_scenarios::Int64
    model_params::Vector{ModelParam}
    rcp_scenario::OptionalValue{String}  # defaults to "45" if not provided
end

"""
Output payload for ADRIA_MODEL_RUN job - Updated to include multiple visualizations
"""
struct AdriaModelRunOutput <: AbstractJobOutput
    # Relative S3 storage location of result set
    output_result_set_path::String
    # Dictionary mapping chart titles to their S3 file paths (JSON serializable)
    available_charts::Dict{String,String}
    # Metadata about generated charts
    chart_metadata::Dict{String,Dict{String,Any}}
end

"""
Handler for ADRIA_MODEL_RUN jobs
"""
struct AdriaModelRunHandler <: AbstractJobHandler end

# ========================
# ADRIA Model Run Handler
# ========================

"""
Process an ADRIA_MODEL_RUN job - Updated version with multiple visualizations
"""
function handle_job(
    ::AdriaModelRunHandler,
    input::AdriaModelRunInput,
    context::JobContext
)::AdriaModelRunOutput
    @info "Starting ADRIA model run" job_id = context.job.id assignment_id =
        context.assignment.id
    start_time = time()

    # Use provided RCP scenario or default to "45"
    rcp_scenario = something(input.rcp_scenario, "45")
    @info "Using RCP scenario: $rcp_scenario"
    @debug "Input parameters" num_scenarios = input.num_scenarios num_custom_params = length(
        input.model_params
    )

    # Define the domain data path
    if input.data_package == "MOORE"
        data_pkg_path = context.config.moore_data_package_path
    elseif input.data_package == "GBR"
        data_pkg_path = context.config.gbr_data_package_path
    else
        throw("Invalid data package input: $(input.data_package)")
    end

    @debug "Data package path configured" path = data_pkg_path

    # Load the domain
    @info "Loading domain data from: $data_pkg_path"
    domain_load_start = time()
    domain = ADRIA.load_domain(data_pkg_path, rcp_scenario)
    domain_load_time = time() - domain_load_start
    @debug "Domain loaded successfully" load_time_seconds = round(
        domain_load_time; digits=2
    )

    # Apply custom parameters if provided
    if !isempty(input.model_params)
        @info "Applying $(length(input.model_params)) custom model parameters"
        @debug "Custom parameters" params = input.model_params
        param_start = time()
        update_domain_with_params!(; domain, params=input.model_params)
        param_time = time() - param_start
        @debug "Custom parameters applied" update_time_seconds = round(param_time; digits=2)
    else
        @debug "No custom parameters provided, using defaults"
    end

    # Generate scenarios with the specified number
    @info "Generating $(input.num_scenarios) scenarios"
    scenario_gen_start = time()
    scenarios = ADRIA.sample(domain, input.num_scenarios)
    scenario_gen_time = time() - scenario_gen_start
    @debug "Scenarios generated" generation_time_seconds = round(
        scenario_gen_time; digits=2
    )

    # Determine unique parent directory 
    @debug "Creating unique working directory" base_dir = context.config.data_scratch_space
    unique_parent_folder = create_unique_folder(;
        base_dir=context.config.data_scratch_space
    )
    work_directory = joinpath(unique_parent_folder, "work")
    upload_directory_path = joinpath(unique_parent_folder, "uploads")
    @debug "Working directories created" parent = unique_parent_folder work = work_directory upload =
        upload_directory_path

    # Create directories
    mkpath(work_directory)
    mkpath(upload_directory_path)
    @debug "Directory structure created successfully"

    # Tell ADRIA to write with this folder as parent
    set_adria_output_dir(work_directory)

    # Run the scenarios (in the above unique parent directory)
    @info "Running scenarios for RCP $rcp_scenario"
    simulation_start = time()
    result = ADRIA.run_scenarios(domain, scenarios, rcp_scenario)
    simulation_time = time() - simulation_start
    @info "ADRIA scenarios completed" simulation_time_seconds = round(
        simulation_time; digits=2
    )

    # Generate ALL registered visualizations
    @info "Generating all registered visualizations"
    viz_start = time()
    charts_dict, metadata_dict = generate_all_visualizations(
        scenarios, result, upload_directory_path
    )
    viz_time = time() - viz_start
    @info "All visualizations generated successfully" total_charts = length(charts_dict) generation_time_seconds = round(
        viz_time; digits=2
    )

    # Move the output result set into a predictable location
    rs_output_name = "result_set"
    @debug "Moving result set to upload directory" target_name = rs_output_name
    move_start = time()
    move_result_set_to_determined_location(;
        target_location=upload_directory_path,
        folder_name=rs_output_name
    )
    move_time = time() - move_start
    @debug "Result set moved successfully" move_time_seconds = round(move_time; digits=2)

    # Clean up memory before upload
    @debug "Cleaning up large objects from memory"
    result = nothing
    domain = nothing
    GC.gc()
    @debug "Memory cleanup completed"

    # Upload results
    @info "Initiating file upload to S3" storage_uri = context.assignment.storage_uri
    upload_start = time()
    upload_directory(
        context.storage_client, upload_directory_path, context.assignment.storage_uri
    )
    upload_time = time() - upload_start
    @info "File upload completed successfully" upload_time_seconds = round(
        upload_time; digits=2
    )

    # Clean up scratch space
    @debug "Cleaning up scratch space" cleanup_path = unique_parent_folder
    cleanup_success = rmdir(unique_parent_folder; verbose=false)
    if cleanup_success
        @debug "Scratch space cleaned up successfully"
    else
        @warn "Failed to clean up scratch space" path = unique_parent_folder
    end

    execution_time = time() - start_time
    total_files = length(charts_dict) + 1  # charts + result_set
    @info "ADRIA model run completed successfully" total_time_seconds = round(
        execution_time; digits=2
    ) files_generated = total_files charts_generated = length(charts_dict)

    return AdriaModelRunOutput(
        rs_output_name,
        charts_dict,
        metadata_dict
    )
end

#
# ====
# INIT
# ====
#

function __init__()
    # Initialize default metrics first
    initialize_default_metrics!()

    # Register the ADRIA_MODEL_RUN job handler
    register_job_handler!(
        ADRIA_MODEL_RUN,
        AdriaModelRunHandler(),
        AdriaModelRunInput,
        AdriaModelRunOutput
    )

    @info "ADRIA Jobs module initialized" registered_metrics = length(METRIC_REGISTRY) handlers_registered =
        1
end
