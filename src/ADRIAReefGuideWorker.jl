module ADRIAReefGuideWorker

# System imports 
using Base.Threads

# Utilities and helpers for assessments
include("utility/utility.jl")

# Processing helper functions
include("processing/processing.jl")

# Visualisation utilities
include("visualisations/visualisations.jl")

# Worker system
include("job_worker/job_worker.jl")

"""
Create and initialize a worker from the environment.

This is a blocking operation until the worker times out.
"""
function start_worker()
    @info "Initializing worker from environment variables..."
    worker = create_worker_from_env()

    # NOTE: you can perform additional setup here if needed. For example, you
    # might want to initialise data, caches or clients.

    # Worker launch
    @info "Starting worker loop from ADRIAReefGuideWorker.jl with $(Threads.nthreads()) threads."
    start(worker)
    @info "Worker closed itself..."
end

export start_worker

end
