# Use the official Airflow image as base
FROM apache/airflow:3.0.4

# Switch to root user to install system dependencies
USER root

# Install system dependencies for image processing
RUN apt-get update && apt-get install -y \
    libgl1-mesa-glx \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libgomp1 \
    libgtk-3-0 \
    libavcodec-dev \
    libavformat-dev \
    libswscale-dev \
    libv4l-dev \
    libxvidcore-dev \
    libx264-dev \
    libjpeg-dev \
    libpng-dev \
    libtiff-dev \
    libatlas-base-dev \
    gfortran \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Set environment variables
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/tasks"
ENV AIRFLOW_HOME=/opt/airflow

# Install Python dependencies directly
RUN pip install --no-cache-dir \
    pandas>=2.0.0 \
    numpy>=1.24.0 \
    Pillow>=10.0.0 \
    apache-airflow>=3.0.0 \
    apache-airflow-providers-celery>=3.0.0 \
    apache-airflow-providers-postgres>=5.0.0 \
    apache-airflow-providers-redis>=3.0.0 \
    python-dateutil>=2.8.0 \
    pytz>=2023.3 \
    opencv-python>=4.8.0 \
    scikit-image>=0.21.0 \
    matplotlib>=3.7.0 \
    seaborn>=0.12.0 \
    structlog>=23.0.0

# Create necessary directories
RUN mkdir -p /opt/airflow/tasks /opt/airflow/data/input /opt/airflow/data/output

# Copy the classification task
COPY --chown=airflow:root tasks/ /opt/airflow/tasks/

# Set proper permissions
RUN chmod -R 755 /opt/airflow/tasks

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Default command
CMD ["airflow", "webserver"]
