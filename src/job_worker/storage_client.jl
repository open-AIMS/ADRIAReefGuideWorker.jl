"""
An abstract storage client and implementation for S3 - which is untested/not
used yet.
"""

"""
Abstract type for storage clients
All concrete storage clients should inherit from this
"""
abstract type StorageClient end

"""
Parse a storage URI into its components
Returns a tuple of (scheme, bucket, path)
"""
function parse_storage_uri(uri::String)::Tuple{String,String,String}
    # Match s3://bucket-name/path or other schemes
    m = match(r"^([a-z0-9]+)://([^/]+)/(.*)$", uri)
    if isnothing(m)
        throw(ArgumentError("Invalid storage URI format: $uri"))
    end
    scheme = m.captures[1]
    bucket = m.captures[2]
    path = m.captures[3]
    return (scheme, bucket, path)
end

"""
S3 Storage Client implementation
"""
struct S3StorageClient <: StorageClient
    region::String
    s3_endpoint::OptionalValue{String}

    # Constructor with defaults
    function S3StorageClient(;
        region::String,
        s3_endpoint::OptionalValue{String}=nothing
    )
        return new(region, s3_endpoint)
    end
end

"""
Upload a file to the specified storage URI
"""
function upload_file(
    client::S3StorageClient,
    local_path::String,
    storage_path::String;
    silent::Bool=false
)::String
    try
        # Parse the storage URI - concat the target path + base path
        scheme, bucket, key = parse_storage_uri(storage_path)
        if scheme != "s3"
            throw(ArgumentError("Expected S3 URI, got $scheme"))
        end

        if !silent
            @debug "Uploading file from $(local_path) to $(storage_path)"
        end

        aws =
            !isnothing(client.s3_endpoint) ?
            # Use minio special config if we need to
            Minio.MinioConfig(
                client.s3_endpoint;
                # TODO would be nicer to pass through config tree
                username=ENV["MINIO_USERNAME"],
                password=ENV["MINIO_PASSWORD"]
            ) :
            # Otherwise use typical AWS config
            AWS.AWSConfig(; region=client.region)

        # Read the file content
        file_data = Base.read(local_path)

        # Upload to S3
        AWSS3.s3_put(aws, bucket, key, file_data)

        # For demonstration purposes only - simulate the upload
        if !silent
            @debug "Uploaded file to $storage_path"
        end
        return storage_path
    catch e
        @error "Failed to upload file to S3: $e" exception = (e, catch_backtrace())
        rethrow(e)
    end
end

"""
Upload an entire directory (recursively) to the specified storage URI base path.

Maintains the directory structure in the remote location. For example, if uploading
a local directory "/tmp/results" containing files:
- "/tmp/results/data.csv"  
- "/tmp/results/plots/chart.png"

To storage path "s3://bucket/experiment_1/", the files will be uploaded as:
- "s3://bucket/experiment_1/data.csv"
- "s3://bucket/experiment_1/plots/chart.png"

# Arguments
- `client::S3StorageClient`: The S3 client to use for uploads
- `local_directory::String`: Path to the local directory to upload
- `storage_base_path::String`: Base storage URI where directory contents should be uploaded

# Returns
- `Vector{String}`: Array of uploaded file storage paths

# Throws
- `ArgumentError`: If local directory doesn't exist or storage URI is invalid
- `SystemError`: If file reading fails
- Various S3-related exceptions if upload fails
"""
function upload_directory(
    client::S3StorageClient,
    local_directory::String,
    storage_base_path::String
)::Vector{String}
    @debug "Starting directory upload" local_directory storage_base_path

    # Validate local directory exists
    if !isdir(local_directory)
        throw(ArgumentError("Local directory does not exist: $local_directory"))
    end

    # Ensure storage base path ends with / for proper concatenation
    storage_base =
        endswith(storage_base_path, "/") ? storage_base_path : storage_base_path * "/"
    @debug "Normalized storage base path" storage_base

    uploaded_files = String[]

    # Walk through all files in the directory recursively
    for (root, dirs, files) in walkdir(local_directory)
        for file in files
            # Full local path to the file
            local_file_path = joinpath(root, file)

            # Calculate relative path from the base directory
            relative_path = relpath(local_file_path, local_directory)

            # Construct storage path (use forward slashes for S3 paths)
            storage_file_path = storage_base * replace(relative_path, "\\" => "/")

            try
                # Upload the individual file
                uploaded_path = upload_file(
                    client, local_file_path, storage_file_path; silent=true
                )
                push!(uploaded_files, uploaded_path)
            catch e
                @error "Failed to upload file" file = local_file_path exception = (
                    e, catch_backtrace()
                )
                rethrow(e)
            end
        end
    end

    @info "Directory upload completed" total_files = length(uploaded_files) local_directory storage_base
    return uploaded_files
end

"""
Factory function to create an S3 storage client based on environment variables
"""
function create_s3_client()::S3StorageClient
    # Get configuration from environment
    region = Base.get(ENV, "AWS_REGION", "ap-southeast-2")
    return S3StorageClient(;
        region=region
    )
end
