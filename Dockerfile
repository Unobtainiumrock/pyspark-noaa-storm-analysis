# ---- Stage 1: The "Builder" Stage ----
# Use a base image with Java 21 and build Spark with native Hadoop libraries
# Using eclipse-temurin as openjdk images are deprecated on Docker Hub
# Note: slim variant is not available for Java 21, using standard jdk image
FROM eclipse-temurin:21-jdk AS builder

# Install system dependencies needed for building
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Set Spark version
ENV SPARK_VERSION=4.0.1
ENV HADOOP_VERSION=3

# Download and install Spark with Hadoop
WORKDIR /tmp
RUN wget -O spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    tar -xzf spark.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark.tgz

# Download and install Hadoop native libraries
RUN wget -O hadoop.tgz "https://archive.apache.org/dist/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz" && \
    tar -xzf hadoop.tgz && \
    mkdir -p /opt/spark/lib/native && \
    cp -r hadoop-3.4.1/lib/native/* /opt/spark/lib/native/ && \
    rm -rf hadoop-3.4.1 hadoop.tgz

# Download Google Cloud Storage connector for Hadoop 3
RUN mkdir -p /opt/spark/jars && \
    wget -O /opt/spark/jars/gcs-connector-hadoop3-2.2.11.jar \
    "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.11.jar"

# Install Python and pip (minimal approach)
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-venv \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv for faster Python package management using --break-system-packages
# This is safe in a Docker container as it's isolated
RUN pip3 install --break-system-packages uv

# Create a virtual environment for our development packages
RUN uv venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Copy requirements and install Python packages
WORKDIR /app
COPY requirements.txt .
RUN uv pip install --no-cache -r requirements.txt

# Install JupyterLab LSP extension for code quality integration
RUN uv pip install --no-cache jupyterlab-lsp

# ---- Stage 2: The "Final" Runtime Stage ----
# Create a lean final image with only runtime dependencies
FROM python:3.11-slim

# Install Java 21 and native Hadoop libraries for performance
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    libsnappy1v5 \
    liblz4-1 \
    zlib1g \
    && rm -rf /var/lib/apt/lists/*

# Install uv for Python package management
RUN pip install uv

# Copy the requirements file and recreate the virtual environment
COPY --from=builder /app/requirements.txt /tmp/requirements.txt
RUN uv venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN uv pip install --no-cache -r /tmp/requirements.txt
RUN uv pip install --no-cache jupyterlab-lsp

# Create a non-root user for better security
RUN useradd --create-home --shell /bin/bash sparkdev
USER sparkdev
WORKDIR /home/sparkdev/app

# Copy the entire Spark installation (includes Hadoop with native libraries)
COPY --from=builder /opt/spark /opt/spark

# Set environment variables for Java, Spark, and Python
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV VIRTUAL_ENV=/opt/venv
# Add venv and Spark binaries to the system's PATH
ENV PATH="/opt/venv/bin:/opt/spark/bin:/opt/spark/sbin:${PATH}"
# Link PySpark with the Python executable in our venv
ENV PYSPARK_PYTHON=/opt/venv/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python
# Set Hadoop native library path to eliminate warnings
ENV LD_LIBRARY_PATH="/opt/spark/lib/native:${LD_LIBRARY_PATH}"

# Expose the port for JupyterLab
EXPOSE 8888

# Default command to start a shell
CMD ["bash"]

