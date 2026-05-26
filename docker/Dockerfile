# Dockerfile for building and testing epistemic-graph
FROM python:3.11-slim

# Install system packages, build tools, and Rust toolchain
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /app

# Pre-install dependencies to utilize Docker cache layers
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source and config files
COPY . .

# Compile Rust bindings and install in-place
RUN pip install -e .

# Run pytest by default
CMD ["pytest"]
