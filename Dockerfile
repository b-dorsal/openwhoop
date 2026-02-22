# Stage 1: Build the Rust application
FROM rust:slim-bookworm AS builder

# Install dependencies required by btleplug and other crates on Debian
RUN apt-get update && apt-get install -y \
    pkg-config \
    libdbus-1-dev \
    libudev-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create a new empty shell project
WORKDIR /usr/src/openwhoop

# Copy over the workspace manifests and source code
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Build the application for release
RUN cargo build --release

# Stage 2: Create the minimal runtime image
FROM debian:bookworm-slim

# Install runtime dependencies needed for connecting to GCP (HTTP/TLS) and Bluetooth components
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libdbus-1-3 \
    libudev1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /usr/src/openwhoop/target/release/openwhoop /usr/local/bin/openwhoop

# Environment variables (Firestore/BigQuery will read PORT and GOOGLE_APPLICATION_CREDENTIALS automatically if provided by Cloud Run)
ENV RUST_LOG="info"

# Expose the default Cloud Run port (Optional: if you map jobs to Cloud Run proxy)
EXPOSE 8080

# Run the unified CLI
ENTRYPOINT ["openwhoop"]
