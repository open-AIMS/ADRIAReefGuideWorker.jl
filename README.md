# ADRIAReefGuideWorker.jl

A Julia-based worker template for the [open-AIMS/reefguide](https://github.com/open-AIMS/reefguide) distributed job processing system. This template provides a foundation for implementing custom job handlers that can process tasks from the ReefGuide job queue.

## Context

This worker template integrates with the ReefGuide ecosystem, connecting to the ReefGuide web API to receive and process distributed computing tasks. The `API_ENDPOINT` environment variable should point to your ReefGuide web API instance.

**For local development**: First spin up the local ReefGuide stack by following the setup instructions in the [ReefGuide README](https://github.com/open-AIMS/reefguide). This will provide the necessary API endpoint and supporting services (database, S3 storage, etc.) that the worker connects to.

## Quick Start

### Prerequisites

- Install Julia 1.11.x using [juliaup](https://github.com/JuliaLang/juliaup):
  ```bash
  curl -fsSL https://install.julialang.org | sh
  juliaup add 1.11
  juliaup default 1.11
  ```

### Setup and Development

#### Local Development

1. Copy the environment template:
   ```bash
   cp .env.local .env
   ```
2. Navigate to the `sandbox/` directory
3. Initialize the project:
   ```bash
   ./init.sh
   ```
4. Start the development environment:
   ```bash
   ./start.sh
   ```

#### Docker Development

1. Copy the environment template:
   ```bash
   cp .env.local .env
   ```
2. Build the Docker image:
   ```bash
   docker build . -t worker
   ```
3. Run the container:
   ```bash
   docker run --env-file .env worker
   ```

## How It Works

### Core Architecture

The worker operates on a polling-based architecture:

1. **Polling**: Continuously polls the ReefGuide API for available jobs
2. **Job Assignment**: Claims jobs that match its configured job types
3. **Processing**: Executes the appropriate handler for each job type
4. **Completion**: Reports results back to the API
5. **Idle Timeout**: Automatically shuts down after a configurable period of inactivity

### Job Handlers

Job processing is handled through a type-safe registry system:

- **Job Types**: Defined as enums in `handlers.jl` (e.g., `TEST`)
- **Input/Output Types**: Strongly typed payloads for each job type
- **Handlers**: Implement the `AbstractJobHandler` interface to process specific job types

### Example: TEST Job Handler

The template includes a complete example:

```julia
# Input payload structure
struct TestInput <: AbstractJobInput
    id::Int64
end

# Output payload structure
struct TestOutput <: AbstractJobOutput
end

# Handler implementation
struct TestHandler <: AbstractJobHandler end

function handle_job(::TestHandler, input::TestInput, context::HandlerContext)::TestOutput
    @debug "Processing test job with id: $(input.id)"
    sleep(10)  # Simulate work
    return TestOutput()
end
```

### Configuration

Configure the worker through environment variables (see `.env.local` for examples):

- **`API_ENDPOINT`**: ReefGuide API base URL
- **`JOB_TYPES`**: Comma-separated list of job types to handle
- **`WORKER_USERNAME/PASSWORD`**: Authentication credentials
- **`AWS_REGION`**: AWS region for S3 storage
- **`S3_ENDPOINT`**: Optional S3-compatible endpoint (for local development)

### Storage Integration

Workers can read from and write to S3-compatible storage:

- Each job assignment includes a unique `storage_uri` for outputs
- Use the provided `HandlerContext` to access storage configuration
- Support for both AWS S3 and local MinIO development environments

## Adding New Job Types

1. Define the job type enum in `handlers.jl`
2. Create input/output payload structs
3. Implement a handler struct and `handle_job` method
4. Register the handler in the `__init__()` function

## Development vs Production

- **Development**: Uses local MinIO for S3 storage and local API endpoint
- **Production**: Runs in AWS ECS Fargate with proper AWS S3 integration
- **Docker**: Includes multi-stage Dockerfile for containerized deployment

## Threading

The worker is designed to be single-threaded per job but can utilize Julia's threading for computationally intensive tasks within job handlers.
