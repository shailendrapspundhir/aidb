FROM ubuntu:22.04

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    protobuf-compiler \
    pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust using rustup
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

# Copy dependency files first for better caching
COPY Cargo.toml Cargo.lock* ./
COPY build.rs ./

# Download dependencies ahead of build
RUN cargo fetch

# Copy source code
COPY src ./src
COPY proto ./proto

# Build the application
RUN cargo build --release

# Run the application
CMD ["./target/release/my_ai_db"]